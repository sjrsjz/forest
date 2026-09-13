"""Fenced Agent checkpoints, model permits and causally closed tool groups.

No database transaction calls a model, plugin or platform. An Actor owns one
conversation; persistent Session/Run identity survives releasing that Actor.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from psycopg.types.json import Jsonb

from forest.agent_protocol import (
    AssistantBlock,
    FunctionCallBlock,
    ModelTurn,
    ThinkingBlock,
    parse_model_turn,
    serialize_model_turn,
)
from forest.errors import ForestError
from forest.models import PreparedModelRequest
from forest.packages import digest_json
from forest.storage.agent_memory import AgentMemoryStore
from forest.storage.agent_payloads import AgentPayloadStore
from forest.storage.agent_specs import AgentSpecStore, spec_metadata, usage_metadata
from forest.storage.control import BotControl
from forest.storage.kernel_payloads import KernelPayloadStore
from forest.storage.messages import ActorLease, MessageStore, _json, _text, _tokens


@dataclass(frozen=True)
class ModelPermit:
    bot_id: str
    run_id: str
    logical_step_id: str
    model_request_id: str
    transport_attempt_id: str
    model_call_epoch: int
    owner_epoch: int
    actor_epoch: int
    host_incarnation: str
    wire_digest: str


@dataclass(frozen=True)
class ToolPermit:
    bot_id: str
    run_id: str
    tool_call_id: str
    operation_id: str
    arguments_digest: str
    authority_trigger_id: str
    principal_ref: str
    owner_epoch: int
    actor_epoch: int
    host_incarnation: str
    attempt_count: int


class AgentStore:
    def __init__(self, messages: MessageStore):
        self.messages, self.store = messages, messages.store
        self.control = BotControl(self.store)
        self.payloads = AgentPayloadStore(self.store)
        self.specs = AgentSpecStore(self.store)

    @staticmethod
    async def guard_tool_call_in(
        conn, bot_id, agent_tool_id, *, input_digest=None, recovery=False
    ):
        """Recheck a tool under the caller's already-held Bot runtime lock.

        No Binding lock is acquired in reverse order. Every writer of these
        authorities holds the runtime lock; fresh SELECTs plus database time
        therefore cover revocation and time spent waiting for business locks.
        """
        row = await (
            await conn.execute(
                "SELECT to_jsonb(t) AS tool,to_jsonb(a) AS agent,to_jsonb(c) AS conversation,"
                "to_jsonb(b) AS binding,to_jsonb(r) AS runtime,to_jsonb(h) AS host,"
                "to_jsonb(s) AS session,cr.status AS run_status,clock_timestamp() AS now "
                "FROM public.bot_agent_tools t JOIN public.bot_agent_runs a USING(bot_id,run_id) "
                "JOIN public.bot_conversations c ON c.bot_id=a.bot_id AND c.conversation_id=a.conversation_id "
                "JOIN public.bot_bindings b ON b.bot_id=c.bot_id AND b.binding_id=c.binding_id "
                "JOIN public.bot_runtime_state r ON r.bot_id=t.bot_id JOIN public.bot_host_lease h ON h.bot_id=t.bot_id "
                "JOIN public.bot_conversation_sessions s ON s.bot_id=a.bot_id AND s.session_id=a.session_id "
                "JOIN public.bot_conversation_runs cr ON cr.bot_id=a.bot_id AND cr.run_id=a.run_id "
                "WHERE t.bot_id=%s AND t.operation_id=%s",
                (bot_id, agent_tool_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("AGENT_TOOL_REVOKED", "Agent tool authority is unavailable", 403)
        tool, head, conversation, binding, runtime, host, session = (
            row[k]
            for k in ("tool", "agent", "conversation", "binding", "runtime", "host", "session")
        )
        from types import SimpleNamespace

        payloads = AgentPayloadStore(SimpleNamespace(bot_id=bot_id))

        def live_time(value):
            return value is not None and datetime.fromisoformat(value) > row["now"]

        if (
            tool["status"] != ("UNKNOWN" if recovery else "DISPATCHING")
            or head["phase"] != ("BLOCKED_UNKNOWN" if recovery else "WAIT_TOOL")
            or tool["logical_step_id"] != head["current_step_id"]
            or (not recovery and (
                tool["owner_epoch"] != head["owner_epoch"]
                or tool["actor_epoch"] != head["actor_epoch"]
                or tool["host_incarnation"] != head["actor_host_incarnation"]
            ))
            or head["actor_epoch"] != conversation["actor_epoch"]
            or head["actor_host_incarnation"] != conversation["actor_host_incarnation"]
            or head["actor_host_incarnation"] != host["incarnation"]
            or binding["host_incarnation"] != host["incarnation"]
            or binding["host_owner_epoch"] != host["owner_epoch"]
            or not live_time(host["expires_at"])
            or not live_time(conversation["actor_expires_at"])
            or not live_time(head["deadline_at"])
            or not conversation["actor_id"]
            or row["run_status"] != "RUNNING"
            or session["active_run_id"] != head["run_id"]
            or conversation["active_session_id"] != head["session_id"]
            or session["session_epoch"] != head["session_epoch"]
            or binding["binding_epoch"] != conversation["actor_binding_epoch"]
            or not binding["enabled"]
            or binding["state"] != "ONLINE"
            or not runtime["data_admission"]
            or runtime["status"] != "ACTIVE"
            or any(
                runtime[k] != conversation["actor_" + k]
                for k in ("policy_revision", "maintenance_epoch", "storage_generation")
            )
        ):
            raise ForestError(
                "AGENT_TOOL_REVOKED", "Agent tool execution lost its current authority", 403
            )
        revision = head["spec_metadata"]["config_revision"]
        current = None
        if revision:
            from forest.storage.agent_config import AgentConfig

            cfg = await AgentConfig.guard_in(conn, SimpleNamespace(
                host=SimpleNamespace(bot_id=bot_id), policy_revision=runtime["policy_revision"]
            ), revision)
            current = cfg["current_values"]
        head = await AgentSpecStore(SimpleNamespace(bot_id=bot_id)).hydrate_in(conn, head)
        tool = await payloads.hydrate_tool_in(conn, tool)
        current = current if current is not None else AgentStore._values(head)
        from forest.storage.bindings import BindingStore

        effective = await BindingStore.current_config_in(
            conn, bot_id, binding["binding_id"], expected_binding_epoch=binding["binding_epoch"]
        )
        binding["config"] = effective["config"]
        await AgentMemoryStore.guard_run_in(conn, bot_id, head["run_id"])
        settings = AgentStore._tool_settings(head, current, tool["tool_name"])
        trigger = await (
            await conn.execute(
                "SELECT t.*,i.author FROM public.bot_conversation_triggers t JOIN public.bot_message_inbox i "
                "ON i.bot_id=t.bot_id AND i.event_id=t.event_id WHERE t.bot_id=%s AND t.trigger_id=%s",
                (bot_id, tool["authority_trigger_id"]),
            )
        ).fetchone()
        if (
            not trigger
            or trigger["claimed_run_id"] != head["run_id"]
            or trigger["status"] != "CLAIMED"
            or trigger["principal_ref"] != tool["principal_ref"]
            or trigger["ingress_seq"] > head["selected_input_end_seq"]
            or tool["arguments"].get("authority_trigger_id") != trigger["trigger_id"]
        ):
            raise ForestError(
                "TOOL_AUTHORITY_INVALID", "Tool source trigger no longer belongs to this Run", 403
            )
        author = trigger["author"]
        if author.get("identity_origin") not in {"adapter_verified", "adapter_anonymous"}:
            raise ForestError(
                "TOOL_AUTHORITY_DENIED", "System triggers do not delegate tool authority", 403
            )
        if settings["authority"] == "binding_admin" and (
            author.get("identity_origin") != "adapter_verified"
            or author.get("anonymous") is not None
            or author.get("is_self")
            or author["external_user_id"] not in binding["config"].get("admin_user_ids", [])
        ):
            raise ForestError(
                "TOOL_AUTHORITY_DENIED", "Tool source is not a current Binding administrator", 403
            )
        if tool["requires_approval"] or settings["approval"] == "always":
            approved = await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_agent_tool_approvals WHERE bot_id=%s AND run_id=%s AND tool_call_id=%s "
                    "AND decision='APPROVE' AND arguments_digest=%s AND authority_trigger_id=%s "
                    "AND principal_ref=%s AND policy_revision=%s AND expires_at>clock_timestamp()",
                    (
                        bot_id,
                        head["run_id"],
                        tool["tool_call_id"],
                        tool["arguments_digest"],
                        tool["authority_trigger_id"],
                        tool["principal_ref"],
                        runtime["policy_revision"],
                    ),
                )
            ).fetchone()
            if not approved:
                raise ForestError(
                    "TOOL_APPROVAL_REQUIRED", "Exact tool approval is absent or expired", 403
                )
        if input_digest is not None and input_digest != digest_json(
            {
                "component": settings["component_id"],
                "service": settings["service"],
                "contract_major": settings.get("contract_major", 1),
                "payload": tool["arguments"].get("arguments"),
            }
        ):
            raise ForestError(
                "TOOL_INPUT_CONFLICT", "Kernel call differs from the approved tool input", 403
            )
        now = (await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone())["now"]
        if any(
            datetime.fromisoformat(value) <= now
            for value in (host["expires_at"], conversation["actor_expires_at"], head["deadline_at"])
        ):
            raise ForestError(
                "AGENT_TOOL_REVOKED", "Agent tool authority expired before commit", 403
            )
        return {**tool, "tool_def": settings, "spec": head["spec"]}

    async def inspect(self, ticket, operation_id, payload):
        if payload != {}:
            raise ForestError("AGENT_CONTROL_INVALID", "Run inspection has no input fields", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "agent.read", operation_id, payload)
            scope = claims.get("source_conversation_id")
            runs = await (
                await conn.execute(
                    "SELECT a.run_id,a.conversation_id,a.session_id,a.phase,a.checkpoint_seq,a.input_revision,"
                    "a.selected_input_end_seq,a.next_step_seq,a.final_step_id,a.final_reply_id,a.error_code,a.deadline_at,a.created_at,"
                    "c.platform,c.channel_kind,c.channel_id,c.group_source_id FROM public.bot_agent_runs a "
                    "JOIN public.bot_conversations c ON c.bot_id=a.bot_id AND c.conversation_id=a.conversation_id "
                    "WHERE a.bot_id=%s AND (%s::text IS NULL OR a.conversation_id=%s) ORDER BY a.created_at DESC LIMIT 200",
                    (self.store.bot_id, scope, scope),
                )
            ).fetchall()
            approvals = await (
                await conn.execute(
                    "SELECT a.*,t.tool_name,t.arguments_payload_id,t.status AS tool_status,r.conversation_id,i.author,"
                    "g.event_id,g.ingress_seq,c.platform,c.channel_kind,c.channel_id,c.group_source_id "
                    "FROM public.bot_agent_tool_approvals a "
                    "JOIN public.bot_agent_tools t USING(bot_id,run_id,tool_call_id) JOIN public.bot_agent_runs r USING(bot_id,run_id) "
                    "JOIN public.bot_conversations c ON c.bot_id=r.bot_id AND c.conversation_id=r.conversation_id "
                    "JOIN public.bot_conversation_triggers g ON g.bot_id=t.bot_id AND g.trigger_id=t.authority_trigger_id "
                    "JOIN public.bot_message_inbox i ON i.bot_id=g.bot_id AND i.event_id=g.event_id "
                    "WHERE a.bot_id=%s AND (%s::text IS NULL OR r.conversation_id=%s) ORDER BY a.expires_at DESC LIMIT 200",
                    (self.store.bot_id, scope, scope),
                )
            ).fetchall()
            unknown_tools = await (
                await conn.execute(
                    "SELECT t.run_id,t.tool_call_id,t.tool_name,t.arguments_payload_id,t.arguments_digest,t.operation_id,t.status,"
                    "t.authority_trigger_id,t.principal_ref,i.author,g.event_id,g.ingress_seq,c.platform,c.channel_kind,c.channel_id,c.group_source_id "
                    "FROM public.bot_agent_tools t JOIN public.bot_agent_runs r USING(bot_id,run_id) "
                    "JOIN public.bot_conversations c ON c.bot_id=r.bot_id AND c.conversation_id=r.conversation_id "
                    "LEFT JOIN public.bot_conversation_triggers g ON g.bot_id=t.bot_id AND g.trigger_id=t.authority_trigger_id "
                    "LEFT JOIN public.bot_message_inbox i ON i.bot_id=g.bot_id AND i.event_id=g.event_id "
                    "WHERE t.bot_id=%s AND t.status='UNKNOWN' AND (%s::text IS NULL OR r.conversation_id=%s) ORDER BY t.created_at LIMIT 200",
                    (self.store.bot_id, scope, scope),
                )
            ).fetchall()
            for tool in (*approvals, *unknown_tools):
                tool["arguments"] = await self.payloads.read_in(
                    conn, tool["arguments_payload_id"], required=False
                )
            budget = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_budget_days WHERE bot_id=%s ORDER BY budget_day DESC LIMIT 7",
                    (self.store.bot_id,),
                )
            ).fetchall()
            run_budgets = await (
                await conn.execute(
                    "SELECT b.* FROM public.bot_agent_tree_budgets b JOIN public.bot_agent_runs a USING(bot_id,run_id) "
                    "WHERE b.bot_id=%s AND (%s::text IS NULL OR a.conversation_id=%s) ORDER BY a.created_at DESC LIMIT 200",
                    (self.store.bot_id, scope, scope),
                )
            ).fetchall()
            await self.control._time(conn, claims)
            return {
                "runs": runs,
                "approvals": approvals,
                "unknown_tools": unknown_tools,
                "budget": budget,
                "run_budgets": run_budgets,
            }

    async def decide_approval(self, ticket, operation_id, payload):
        payload = _json(payload)
        if set(payload) != {"approval_id", "decision", "arguments_digest"} or payload[
            "decision"
        ] not in {"APPROVE", "REJECT"}:
            raise ForestError(
                "AGENT_CONTROL_INVALID",
                "Approval requires its exact parameter digest and decision",
                422,
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "agent.approval", operation_id, payload
            )
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            approval = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_tool_approvals WHERE bot_id=%s AND approval_id=%s FOR UPDATE",
                    (self.store.bot_id, payload["approval_id"]),
                )
            ).fetchone()
            if not approval:
                raise ForestError("APPROVAL_NOT_FOUND", "Tool approval is unavailable", 404)
            head = await self._head(conn, approval["run_id"])
            tool = await self._tool(conn, head["run_id"], approval["tool_call_id"])
            if (
                head["phase"] != "WAIT_APPROVAL"
                or tool["status"] != "WAIT_APPROVAL"
                or approval["decision"] is not None
                or approval["arguments_digest"] != payload["arguments_digest"]
                or tool["arguments_digest"] != payload["arguments_digest"]
                or approval["policy_revision"] != claims["policy_revision"]
                or approval["expires_at"] <= await self.store._now(conn)
            ):
                raise ForestError(
                    "APPROVAL_CONFLICT",
                    "Tool approval changed, expired, or no longer owns the Run",
                    409,
                )
            await conn.execute(
                "UPDATE public.bot_agent_tool_approvals SET decision=%s,decided_by=%s,operation_id=%s,decided_at=clock_timestamp() "
                "WHERE bot_id=%s AND approval_id=%s",
                (
                    payload["decision"],
                    claims["subject_id"],
                    operation_id,
                    self.store.bot_id,
                    approval["approval_id"],
                ),
            )
            # Runner resumes the same tool and observes the exact persisted decision.
            await self._checkpoint(conn, head, phase="WAIT_TOOL")
            await conn.execute(
                "UPDATE public.bot_conversations SET mailbox_revision=mailbox_revision+1 WHERE bot_id=%s AND conversation_id=%s",
                (self.store.bot_id, head["conversation_id"]),
            )
            result = {
                "approval_id": approval["approval_id"],
                "run_id": head["run_id"],
                "decision": payload["decision"],
            }
            await self.control._record(conn, claims, result)
            await self.control._time(conn, claims)
            if approval["expires_at"] <= await self.store._now(conn):
                raise ForestError(
                    "APPROVAL_CONFLICT", "Approval expired while accepting the decision", 409
                )
            return result

    async def resolve(self, ticket, operation_id, payload):
        payload = _json(payload)
        if set(payload) != {
            "run_id",
            "tool_call_id",
            "expected_checkpoint_seq",
            "arguments_digest",
            "status",
            "result",
        } or payload["status"] not in {"SUCCEEDED", "FAILED"}:
            raise ForestError(
                "AGENT_CONTROL_INVALID",
                "Resolution must identify one exact unknown tool outcome",
                422,
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "agent.resolve", operation_id, payload)
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            head = await self._head(conn, payload["run_id"])
            tool = await self._tool(conn, head["run_id"], payload["tool_call_id"])
            if (
                head["phase"] != "BLOCKED_UNKNOWN"
                or head["checkpoint_seq"] != payload["expected_checkpoint_seq"]
                or tool["status"] != "UNKNOWN"
                or tool["arguments_digest"] != payload["arguments_digest"]
            ):
                raise ForestError(
                    "AGENT_RESOLUTION_CONFLICT", "Unknown tool or checkpoint changed", 409
                )
            result_digest = digest_json([payload["status"], payload["result"]])
            await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, head["run_id"])
            original = await self.control.admitted_input_in(conn, claims)
            result_ref = await self.payloads.tool_result_in(
                conn, tool, payload["result"], status=payload["status"], parents=(original.payload_id,)
            )
            await conn.execute(
                "UPDATE public.bot_agent_tools SET status=%s,result_payload_id=%s,result_digest=%s WHERE bot_id=%s AND run_id=%s AND tool_call_id=%s",
                (
                    payload["status"],
                    result_ref,
                    result_digest,
                    self.store.bot_id,
                    head["run_id"],
                    tool["tool_call_id"],
                ),
            )
            head = await self._checkpoint(conn, head, phase=await self._tool_phase(conn, head))
            result = {
                "run_id": head["run_id"],
                "tool_call_id": tool["tool_call_id"],
                "status": payload["status"],
                "checkpoint_seq": head["checkpoint_seq"],
            }
            await self.control._record(conn, claims, result)
            await self.control._time(conn, claims)
            return result

    async def cancel(self, ticket, operation_id, payload):
        payload = _json(payload)
        if set(payload) != {"run_id", "expected_checkpoint_seq", "reason"}:
            raise ForestError(
                "AGENT_CONTROL_INVALID", "Cancellation requires the displayed Run checkpoint", 422
            )
        _text(payload["reason"], "reason", 1024)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "agent.cancel", operation_id, payload)
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            head = await self._head(conn, payload["run_id"])
            if head["checkpoint_seq"] != payload["expected_checkpoint_seq"] or head["phase"] in {
                "COMPLETED",
                "FAILED",
                "CANCELLED",
            }:
                raise ForestError(
                    "AGENT_CANCEL_CONFLICT", "Run checkpoint changed or is already terminal", 409
                )
            conversation = await self.messages._conversation(conn, head["conversation_id"])
            session = await self.messages._session(conn, conversation)
            if not session or session["active_run_id"] != head["run_id"]:
                raise ForestError("RUN_NOT_ACTIVE", "Run does not own this Session", 409)
            try:
                await self._flush(conn, head)
            except ForestError as exc:
                if exc.code != "AGENT_MEMORY_REVOKED":
                    raise
            await self._abandon(conn, head)
            await conn.execute(
                "UPDATE public.bot_conversation_runs SET status='CANCELLED',completed_at=clock_timestamp() WHERE bot_id=%s AND run_id=%s",
                (self.store.bot_id, head["run_id"]),
            )
            await conn.execute(
                "UPDATE public.bot_conversation_triggers SET status='CANCELLED',terminal_reason=%s WHERE bot_id=%s AND claimed_run_id=%s AND status='CLAIMED'",
                (payload["reason"], self.store.bot_id, head["run_id"]),
            )
            await self.messages._after_run(conn, conversation, session)
            await conn.execute(
                "UPDATE public.bot_conversations SET actor_epoch=actor_epoch+1,actor_id=NULL,actor_expires_at=clock_timestamp(),mailbox_revision=mailbox_revision+1 WHERE bot_id=%s AND conversation_id=%s",
                (self.store.bot_id, head["conversation_id"]),
            )
            head = await self._checkpoint(
                conn, head, phase="CANCELLED", error_code=payload["reason"]
            )
            result = {
                "run_id": head["run_id"],
                "phase": head["phase"],
                "checkpoint_seq": head["checkpoint_seq"],
            }
            await self.control._record(conn, claims, result)
            await self.control._time(conn, claims)
            return result

    async def _head(self, conn, run_id):
        head = await (
            await conn.execute(
                "SELECT * FROM public.bot_agent_runs WHERE bot_id=%s AND run_id=%s FOR UPDATE",
                (self.store.bot_id, run_id),
            )
        ).fetchone()
        if head is None:
            raise ForestError("AGENT_RUN_MISSING", "Persistent Agent run is not registered", 404)
        return head

    async def _guard(self, conn, actor, run_id, *, terminal=False, owner=True):
        binding, conversation = await self.messages._actor(conn, actor)
        session = await self.messages._session(conn, conversation)
        run = await (
            await conn.execute(
                "SELECT * FROM public.bot_conversation_runs WHERE bot_id=%s AND run_id=%s FOR UPDATE",
                (self.store.bot_id, run_id),
            )
        ).fetchone()
        head = await self._head(conn, run_id)
        if (
            run is None
            or run["conversation_id"] != actor.conversation_id
            or head["conversation_id"] != actor.conversation_id
        ):
            raise ForestError(
                "AGENT_SCOPE_MISMATCH", "Agent run belongs to another conversation", 403
            )
        if not terminal and (
            not session
            or session["session_id"] != head["session_id"]
            or session["session_epoch"] != head["session_epoch"]
            or session["active_run_id"] != run_id
            or run["status"] != "RUNNING"
        ):
            raise ForestError("RUN_NOT_ACTIVE", "Agent no longer owns this foreground Session", 409)
        if head["input_revision"] != run["input_revision"]:
            raise ForestError(
                "INPUT_REVISION_CONFLICT", "Agent checkpoint input differs from Run", 409
            )
        if owner and (
            head["actor_epoch"] != actor.actor_epoch
            or head["actor_host_incarnation"] != actor.host.incarnation
        ):
            raise ForestError(
                "STALE_AGENT_OWNER", "Agent execution must acquire this checkpoint", 409
            )
        return binding, conversation, session, run, head

    async def _configuration(self, conn, actor, head):
        revision = head["spec_metadata"]["config_revision"]
        current = None
        if revision > 0:
            from forest.storage.agent_config import AgentConfig

            saved = await AgentConfig.guard_in(conn, actor, revision)
            current = {
                **saved["current_values"],
                "daily_token_budget": saved["daily_token_budget"],
            }
        head.update(await self.specs.hydrate_in(conn, head))
        from forest.storage.bindings import BindingStore

        try:
            effective = await BindingStore.current_config_in(
                conn, self.store.bot_id, actor.binding_id, expected_binding_epoch=actor.binding_epoch
            )
        except ForestError as exc:
            if exc.code == "CONTROL_CONTENT_REVOKED":
                raise ForestError("AGENT_CONFIGURATION_REVOKED", "Binding configuration is unavailable", 409) from None
            raise
        head["_binding_config"] = effective["config"]
        return current if current is not None else self._values(head)

    @staticmethod
    def _values(head):
        from forest.agent_settings import project_stored_agent_values

        # Revision-zero specs predate the Web schema and intentionally admit
        # fractional test/provider deadlines. SQL receipts and ensure_run bound
        # their execution-critical values; this projection only retires fields.
        return project_stored_agent_values(head["spec"].get("values", head["spec"]))

    async def _checkpoint(self, conn, head, *, phase=None, error_code=None):
        head = await (
            await conn.execute(
                "UPDATE public.bot_agent_runs SET checkpoint_seq=checkpoint_seq+1,phase=COALESCE(%s,phase),"
                "error_code=%s WHERE bot_id=%s AND run_id=%s RETURNING *",
                (phase, error_code, self.store.bot_id, head["run_id"]),
            )
        ).fetchone()
        payload = {
            key: head[key]
            for key in (
                "run_id",
                "session_id",
                "session_epoch",
                "owner_epoch",
                "checkpoint_seq",
                "phase",
                "next_step_seq",
                "current_step_id",
                "published_step_seq",
                "input_revision",
                "selected_input_end_seq",
                "final_step_id",
                "final_input_revision",
                "final_reply_id",
                "error_code",
                "spec_digest",
                "tool_iteration_count",
                "thinking_iteration_count",
                "protocol_repair_count",
            )
        }
        await conn.execute(
            "INSERT INTO public.bot_agent_checkpoints(bot_id,run_id,checkpoint_seq,phase,payload,checksum) VALUES (%s,%s,%s,%s,%s,%s)",
            (
                self.store.bot_id,
                head["run_id"],
                head["checkpoint_seq"],
                head["phase"],
                Jsonb(payload),
                digest_json(payload),
            ),
        )
        return head

    async def ensure_run(self, actor: ActorLease, run_id: str, spec: dict | None) -> dict:
        spec = _json(spec) if spec is not None else None
        if spec is not None and (not isinstance(spec, dict) or any(
            key in spec for key in ("api_key", "secret", "ciphertext")
        )):
            raise ForestError(
                "AGENT_SPEC_INVALID", "Run specification cannot contain provider secrets", 422
            )
        values = spec.get("values", spec) if spec is not None else {}
        if not isinstance(values, dict) or any(
            key in values for key in ("api_key", "secret", "ciphertext", "secret_ciphertext")
        ):
            raise ForestError(
                "AGENT_SPEC_INVALID", "Run values must be an object without provider secrets", 422
            )
        for key, default, maximum in (
            ("max_steps", 16, 128),
            ("run_timeout", 600, 86400),
            ("daily_token_budget", 1000000, 10**10),
            ("max_output_tokens", 4096, 262144),
        ):
            value = values.get(key, default)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ForestError("AGENT_SPEC_INVALID", "Invalid bounded Agent configuration", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, conversation = await self.messages._actor(conn, actor)
            session = await self.messages._session(conn, conversation)
            run = await self.messages._run(conn, session)
            if not run or run["run_id"] != run_id or run["status"] != "RUNNING":
                raise ForestError("RUN_NOT_ACTIVE", "Agent requires the active Run", 409)
            head = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_runs WHERE bot_id=%s AND run_id=%s FOR UPDATE",
                    (self.store.bot_id, run_id),
                )
            ).fetchone()
            if head is None:
                if spec is None:
                    raise ForestError("AGENT_SPEC_INVALID", "New Run requires its actual specification", 422)
                metadata = spec_metadata(spec)
                parents, sources = (), ()
                if metadata["config_revision"] > 0:
                    from forest.storage.agent_config import AgentConfig

                    actual = await AgentConfig.guard_in(conn, actor, metadata["config_revision"])
                    if (
                        values != actual["stored_values"]
                        or metadata["secret_revision"] != actual["secret_revision"]
                    ):
                        raise ForestError("AGENT_SPEC_INVALID", "Specification differs from its configured revision", 409)
                    parents = (actual["values_payload_id"],)
                else:
                    from forest.storage.source_domains import SourceDomainStore

                    await self.payloads.epoch_in(conn)
                    sources = (await SourceDomainStore(self.store).source_in(
                        conn, "AGENT_SPEC", "agent-spec:" + digest_json([self.store.bot_id, run_id])
                    ),)
                ref = await self.specs.put_in(conn, run_id, spec, parents=parents, sources=sources)
                head = await (
                    await conn.execute(
                        "INSERT INTO public.bot_agent_runs(bot_id,run_id,conversation_id,session_id,session_epoch,spec,spec_digest,"
                        "actor_epoch,actor_host_incarnation,input_revision,selected_input_end_seq,deadline_at,spec_payload_id,spec_payload_digest,spec_metadata) "
                        "VALUES (%s,%s,%s,%s,%s,NULL,%s,%s,%s,%s,%s,clock_timestamp()+%s*interval '1 second',%s,%s,%s) RETURNING *",
                        (
                            self.store.bot_id,
                            run_id,
                            actor.conversation_id,
                            session["session_id"],
                            session["session_epoch"],
                            digest_json(spec),
                            actor.actor_epoch,
                            actor.host.incarnation,
                            run["input_revision"],
                            run["selected_input_end_seq"],
                            values.get("run_timeout", 600),
                            ref.payload_id,
                            ref.digest,
                            Jsonb(metadata),
                        ),
                    )
                ).fetchone()
                await self._configuration(conn, actor, head)
                head = await self._checkpoint(conn, head)
            elif (
                head["actor_epoch"] != actor.actor_epoch
                or head["actor_host_incarnation"] != actor.host.incarnation
            ):
                head = await (
                    await conn.execute(
                        "UPDATE public.bot_agent_runs SET owner_epoch=owner_epoch+1,actor_epoch=%s,actor_host_incarnation=%s "
                        "WHERE bot_id=%s AND run_id=%s RETURNING *",
                        (actor.actor_epoch, actor.host.incarnation, self.store.bot_id, run_id),
                    )
                ).fetchone()
                head = await self._recover_owner(conn, head)
                head = await self._checkpoint(conn, head)
            if head["deadline_at"] <= await self.store._now(conn) and head["phase"] not in {
                "BLOCKED_UNKNOWN",
                "FINAL_READY",
                "COMPLETED",
                "FAILED",
                "CANCELLED",
            }:
                # A deadline may close only a known continuation. Uncertain
                # effects retain the Session until explicit reconciliation.
                pending = await self._has_unknown_in(conn, run_id)
                await self._abandon(conn, head)
                if pending:
                    head = await self._checkpoint(
                        conn, head, phase="BLOCKED_UNKNOWN", error_code="AGENT_DEADLINE_EXCEEDED"
                    )
                else:
                    await self._flush(conn, head)
                    await self.messages._end_run_in(
                        conn, actor, run_id=run_id, reason="AGENT_DEADLINE_EXCEEDED"
                    )
                    head = await self._checkpoint(
                        conn, head, phase="FAILED", error_code="AGENT_DEADLINE_EXCEEDED"
                    )
            await self.messages._actor_final(conn, actor, binding)
            return await self._expanded(conn, head)

    async def _expanded(self, conn, head):
        required = head["phase"] not in {"FAILED", "CANCELLED", "COMPLETED", "BLOCKED_UNKNOWN"}
        result = await self.specs.hydrate_in(conn, head, required=required)
        result["current_step"] = await (
            await conn.execute(
                "SELECT * FROM public.bot_agent_steps WHERE bot_id=%s AND logical_step_id=%s",
                (self.store.bot_id, head["current_step_id"]),
            )
        ).fetchone()
        result["current_step"] = await self.payloads.hydrate_step_in(
            conn, result["current_step"], required=required
        )
        result["tools"] = await (
            await conn.execute(
                "SELECT *,tool_name AS name,operation_id AS tool_id FROM public.bot_agent_tools WHERE bot_id=%s AND logical_step_id=%s ORDER BY call_index",
                (self.store.bot_id, head["current_step_id"]),
            )
        ).fetchall()
        result["tools"] = [
            await self.payloads.hydrate_tool_in(conn, t, required=required) for t in result["tools"]
        ]
        if result["current_step"] and result["current_step"]["transport_attempt_id"]:
            result["current_step"]["attempt"] = await (
                await conn.execute(
                    "SELECT a.*,m.manifest_payload_id FROM public.bot_model_transport_attempts a JOIN public.bot_model_request_manifests m "
                    "USING(bot_id,model_request_id) WHERE a.bot_id=%s AND a.transport_attempt_id=%s",
                    (self.store.bot_id, result["current_step"]["transport_attempt_id"]),
                )
            ).fetchone()
            result["current_step"]["attempt"] = await self.payloads.hydrate_attempt_in(
                conn, result["current_step"]["attempt"], required=required
            )
        return result

    async def load(self, actor, run_id):
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _ = await self.messages._actor(conn, actor)
            exists = await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_agent_runs WHERE bot_id=%s AND run_id=%s",
                    (self.store.bot_id, run_id),
                )
            ).fetchone()
            if not exists:
                await self.messages._actor_final(conn, actor, binding)
                return None
            binding, _, _, _, head = await self._guard(
                conn, actor, run_id, terminal=True, owner=False
            )
            result = await self._expanded(conn, head)
            await self.messages._actor_final(conn, actor, binding)
            return result

    async def _recover_owner(self, conn, head):
        if head["phase"] == "MODEL_PENDING":
            step = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_steps WHERE bot_id=%s AND logical_step_id=%s FOR UPDATE",
                    (self.store.bot_id, head["current_step_id"]),
                )
            ).fetchone()
            attempt = await (
                await conn.execute(
                    "SELECT * FROM public.bot_model_transport_attempts WHERE bot_id=%s AND transport_attempt_id=%s FOR UPDATE",
                    (self.store.bot_id, step["transport_attempt_id"]),
                )
            ).fetchone()
            unknown = attempt["status"] == "DISPATCHING"
            await conn.execute(
                "UPDATE public.bot_model_transport_attempts SET status=%s WHERE bot_id=%s AND transport_attempt_id=%s",
                (
                    "UNKNOWN" if unknown else "NOT_SENT",
                    self.store.bot_id,
                    attempt["transport_attempt_id"],
                ),
            )
            await self._settle(
                conn,
                attempt["transport_attempt_id"],
                status="UNKNOWN" if unknown else "NOT_SENT",
                usage=None,
            )
            if not unknown:
                await conn.execute(
                    "UPDATE public.bot_agent_steps SET status='RETRY_MODEL' WHERE bot_id=%s AND logical_step_id=%s",
                    (self.store.bot_id, head["current_step_id"]),
                )
            head = await self._checkpoint(
                conn, head, phase="BLOCKED_UNKNOWN" if unknown else "RETRY_MODEL"
            )
        dispatched = await (
            await conn.execute(
                "UPDATE public.bot_agent_tools SET status='UNKNOWN' WHERE bot_id=%s AND run_id=%s AND status='DISPATCHING' RETURNING tool_call_id",
                (self.store.bot_id, head["run_id"]),
            )
        ).fetchall()
        if dispatched:
            head = await self._checkpoint(conn, head, phase="BLOCKED_UNKNOWN")
        return head

    async def _closed(self, conn, head):
        if head["phase"] not in {"READY_MODEL", "WAIT_INPUT"}:
            raise ForestError(
                "AGENT_BOUNDARY_CLOSED",
                "Model/tool group has not reached its next input boundary",
                409,
            )
        pending = await (
            await conn.execute(
                "SELECT 1 FROM public.bot_agent_steps WHERE bot_id=%s AND run_id=%s AND status NOT IN ('CLOSED','FAILED') LIMIT 1",
                (self.store.bot_id, head["run_id"]),
            )
        ).fetchone()
        if pending:
            raise ForestError("AGENT_BOUNDARY_CLOSED", "A model or tool group is still open", 409)

    async def _flush(self, conn, head):
        await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, head["run_id"])
        conversation = await self.messages._conversation(conn, head["conversation_id"])
        rows = await (
            await conn.execute(
                "SELECT * FROM public.bot_agent_steps WHERE bot_id=%s AND run_id=%s AND status='CLOSED' "
                "AND published_context_revision IS NULL ORDER BY step_seq FOR UPDATE",
                (self.store.bot_id, head["run_id"]),
            )
        ).fetchall()
        revision = conversation["context_revision"]
        for step in rows:
            step = await self.payloads.hydrate_step_in(conn, step)
            revision += 1
            await conn.execute(
                "INSERT INTO public.bot_context_units(bot_id,conversation_id,unit_id,context_revision,unit_index,kind,"
                "logical_step_id,payload_id,estimated_tokens) VALUES (%s,%s,%s,%s,0,'RUN_STEP',%s,%s,%s)",
                (
                    self.store.bot_id,
                    head["conversation_id"],
                    step["logical_step_id"],
                    revision,
                    step["logical_step_id"],
                    step["closed_parts_payload_id"],
                    _tokens(step["closed_parts"]),
                ),
            )
            await conn.execute(
                "UPDATE public.bot_agent_steps SET published_context_revision=%s WHERE bot_id=%s AND logical_step_id=%s",
                (revision, self.store.bot_id, step["logical_step_id"]),
            )
        if rows:
            await conn.execute(
                "UPDATE public.bot_conversations SET context_revision=%s WHERE bot_id=%s AND conversation_id=%s",
                (revision, self.store.bot_id, head["conversation_id"]),
            )
            await conn.execute(
                "UPDATE public.bot_agent_runs SET published_step_seq=%s WHERE bot_id=%s AND run_id=%s",
                (rows[-1]["step_seq"], self.store.bot_id, head["run_id"]),
            )

    async def boundary(self, actor, run_id, operation_id):
        _text(operation_id, "operation_id")
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, conversation, _, _, head = await self._guard(conn, actor, run_id)
            await self._closed(conn, head)
            await self._configuration(conn, actor, head)
            eligible = await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_conversation_triggers WHERE bot_id=%s AND conversation_id=%s "
                    "AND status='PENDING' AND ingress_seq<=%s LIMIT 1",
                    (
                        self.store.bot_id,
                        actor.conversation_id,
                        conversation["classified_through_seq"],
                    ),
                )
            ).fetchone()
            if eligible:
                await self._flush(conn, head)
            selection = await self.messages._select_input_in(
                conn, actor, operation_id, mode="SAFE_POINT"
            )
            run = await (
                await conn.execute(
                    "SELECT * FROM public.bot_conversation_runs WHERE bot_id=%s AND run_id=%s",
                    (self.store.bot_id, run_id),
                )
            ).fetchone()
            await conn.execute(
                "UPDATE public.bot_agent_runs SET input_revision=%s,selected_input_end_seq=%s,wait_classified_seq=%s "
                "WHERE bot_id=%s AND run_id=%s",
                (
                    run["input_revision"],
                    run["selected_input_end_seq"],
                    conversation["classified_through_seq"],
                    self.store.bot_id,
                    run_id,
                ),
            )
            head = await self._checkpoint(
                conn,
                head,
                phase="WAIT_INPUT" if selection["status"] == "WAIT_INPUT" else "READY_MODEL",
            )
            await self.messages._actor_final(conn, actor, binding)
            return {**head, "selection": selection, "status": selection["status"]}

    async def snapshot(self, actor, run_id):
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _, _, _, head = await self._guard(conn, actor, run_id)
            await self._configuration(conn, actor, head)
            if head["phase"] == "WAIT_INPUT":
                raise ForestError(
                    "AGENT_INPUT_NOT_READY",
                    "Classify the pending input gap before preparing another model",
                    409,
                )
            if head["phase"] in {"PREPARING_MODEL", "RETRY_MODEL"}:
                await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, run_id)
                expanded = await self._expanded(conn, head)
                await self.messages._actor_final(conn, actor, binding)
                return expanded["current_step"]["snapshot"]
            await self._closed(conn, head)
            values = self._values(head)
            if head["next_step_seq"] > values.get("max_steps", 16) or head[
                "deadline_at"
            ] <= await self.store._now(conn):
                raise ForestError("AGENT_BUDGET_EXCEEDED", "Run step/time budget is exhausted", 409)
            context = await self.messages._context_in(conn, actor, run_id=run_id)
            if context["budget_exceeded"]:
                raise ForestError(
                    "INPUT_BUDGET_EXCEEDED", "Required context exceeds its canonical budget", 409
                )
            units = []
            for unit in context["units"]:
                if unit["kind"] == "RUN_STEP":
                    units.extend(unit["step_parts"])
                else:
                    units.append(
                        {
                            "unit_id": unit["unit_id"],
                            "role": unit["context_role"],
                            "source": unit["source"],
                            "author": unit["author"],
                            "parts": unit["segments"]
                            if unit["kind"] == "INPUT"
                            else [part for group in unit["reply_parts"] or [] for part in group],
                        }
                    )
            private = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_steps WHERE bot_id=%s AND run_id=%s AND status='CLOSED' "
                    "AND published_context_revision IS NULL ORDER BY step_seq",
                    (self.store.bot_id, run_id),
                )
            ).fetchall()
            private = [await self.payloads.hydrate_step_in(conn, row) for row in private]
            for row in private:
                units.extend(row["closed_parts"])
            batches = await (
                await conn.execute(
                    "SELECT input_batch_id FROM public.bot_context_input_batches WHERE bot_id=%s AND run_id=%s ORDER BY input_revision",
                    (self.store.bot_id, run_id),
                )
            ).fetchall()
            step_id = "model-step:" + digest_json(
                [self.store.bot_id, run_id, head["next_step_seq"]]
            )
            snapshot = {
                "bot_id": self.store.bot_id,
                "run_id": run_id,
                "logical_step_id": step_id,
                "input_revision": head["input_revision"],
                "selected_input_end_seq": head["selected_input_end_seq"],
                "context_revision": context["context_revision"],
                "context_view_revision": context["context_view_revision"],
                "trigger_ids": [t["trigger_id"] for t in context["triggers"]],
                "triggers": context["triggers"],
                "input_batch_ids": [b["input_batch_id"] for b in batches],
                "units": units,
                "conversation": context["conversation"],
                "archive": context["archive"],
                "required_unit_ids": [
                    u["unit_id"]
                    for u in context["units"]
                    if u["event_id"] in {t["event_id"] for t in context["triggers"]}
                ]
                + [u["unit_id"] for row in private for u in row["closed_parts"]]
                + [
                    p["unit_id"]
                    for u in context["units"]
                    if u["kind"] == "RUN_STEP" and u["step_run_id"] == run_id
                    for p in u["step_parts"]
                ],
            }
            snapshot = _json(snapshot, maximum=4 * 1024 * 1024)
            snapshot_ref = await self.payloads.put_in(
                conn,
                "model.snapshot",
                step_id,
                snapshot,
                parents=(head["spec_payload_id"], *context["payload_ids"], *[r["closed_parts_payload_id"] for r in private]),
            )
            await conn.execute(
                "INSERT INTO public.bot_agent_steps(bot_id,run_id,logical_step_id,step_seq,input_revision,selected_input_end_seq,snapshot_payload_id,snapshot_digest) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    run_id,
                    step_id,
                    head["next_step_seq"],
                    head["input_revision"],
                    head["selected_input_end_seq"],
                    snapshot_ref,
                    digest_json(snapshot),
                ),
            )
            await conn.execute(
                "UPDATE public.bot_agent_runs SET current_step_id=%s,next_step_seq=next_step_seq+1 WHERE bot_id=%s AND run_id=%s",
                (step_id, self.store.bot_id, run_id),
            )
            await self._checkpoint(conn, head, phase="PREPARING_MODEL")
            await self.messages._actor_final(conn, actor, binding)
            return snapshot

    async def replay_prepared(self, actor, run_id, transport_attempt_id=None):
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _, _, _, head = await self._guard(conn, actor, run_id)
            await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, run_id)
            if head["phase"] != "RETRY_MODEL":
                raise ForestError(
                    "MODEL_RETRY_FORBIDDEN", "Only a confirmed unsent request can retry", 409
                )
            expanded = await self._expanded(conn, head)
            attempt = expanded["current_step"]["attempt"]
            if (
                transport_attempt_id is not None
                and attempt["transport_attempt_id"] != transport_attempt_id
            ):
                raise ForestError("MODEL_ATTEMPT_STALE", "Model retry names another attempt", 409)
            metadata = attempt["prepared_metadata"]
            await self.messages._actor_final(conn, actor, binding)
            return PreparedModelRequest(
                attempt["model_request_id"],
                str(uuid4()),
                bytes(attempt["request_body"]),
                json.dumps(attempt["manifest"], ensure_ascii=False),
                attempt["wire_digest"],
                metadata["estimated_input_tokens"],
                metadata["max_output_tokens"],
                metadata["stream"],
                metadata["serializer_revision"],
            )

    @staticmethod
    def _permit(attempt):
        return ModelPermit(**{key: attempt[key] for key in ModelPermit.__dataclass_fields__})

    async def prepare_model(self, actor, run_id, operation_id, prepared: PreparedModelRequest):
        _text(operation_id, "operation_id")
        prepared.verify()
        manifest = _json(prepared.manifest, maximum=24 * 1024 * 1024)
        if (
            type(prepared.estimated_input_tokens) is not int
            or prepared.estimated_input_tokens < 0
            or type(prepared.max_output_tokens) is not int
            or prepared.max_output_tokens < 1
        ):
            raise ForestError("MODEL_BUDGET_INVALID", "A finite token reservation is required", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _, _, _, head = await self._guard(conn, actor, run_id)
            current = await self._configuration(conn, actor, head)
            await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, run_id)
            previous = await (
                await conn.execute(
                    "SELECT * FROM public.bot_model_transport_attempts WHERE bot_id=%s AND operation_id=%s",
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if previous:
                if (
                    previous["wire_digest"] != prepared.payload_digest
                    or previous["transport_attempt_id"] != prepared.transport_attempt_id
                    or previous["model_request_id"] != prepared.request_id
                ):
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Model permit already binds another request", 409
                    )
                await self.messages._actor_final(conn, actor, binding)
                return self._permit(previous)
            if head["phase"] not in {"PREPARING_MODEL", "RETRY_MODEL"}:
                raise ForestError(
                    "MODEL_ALREADY_PENDING", "A model request already owns this step", 409
                )
            step = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_steps WHERE bot_id=%s AND logical_step_id=%s FOR UPDATE",
                    (self.store.bot_id, head["current_step_id"]),
                )
            ).fetchone()
            for key in (
                "bot_id",
                "run_id",
                "logical_step_id",
                "input_revision",
                "selected_input_end_seq",
                "context_revision",
                "context_view_revision",
            ):
                if step.get("snapshot") is None:
                    step = await self.payloads.hydrate_step_in(conn, step)
                if manifest.get(key) != step["snapshot"][key]:
                    raise ForestError(
                        "MODEL_INPUT_FENCE",
                        "Model manifest differs from its frozen step input",
                        409,
                    )
            if manifest.get("input_digest") != digest_json(step["snapshot"]) or any(
                manifest.get(k) != step["snapshot"][k] for k in ("trigger_ids", "input_batch_ids")
            ):
                raise ForestError(
                    "MODEL_INPUT_FENCE",
                    "Model source references differ from the frozen snapshot",
                    409,
                )
            values = self._values(head)
            if prepared.max_output_tokens > values.get("max_output_tokens", 4096) or head[
                "deadline_at"
            ] <= await self.store._now(conn):
                raise ForestError(
                    "AGENT_BUDGET_EXCEEDED", "Model exceeds Run output/time budget", 409
                )
            await AgentMemoryStore.check_manifest_in(conn, self.store.bot_id, step, manifest)
            if head["phase"] == "RETRY_MODEL":
                old = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_model_transport_attempts WHERE bot_id=%s AND transport_attempt_id=%s",
                        (self.store.bot_id, step["transport_attempt_id"]),
                    )
                ).fetchone()
                if (
                    old["status"] != "NOT_SENT"
                    or old["wire_digest"] != prepared.payload_digest
                    or old["model_request_id"] != prepared.request_id
                ):
                    raise ForestError(
                        "MODEL_RETRY_FORBIDDEN",
                        "Retry must preserve the confirmed-unsent frozen request",
                        409,
                    )
                if step["model_call_epoch"] >= 3:
                    raise ForestError(
                        "MODEL_RETRY_EXHAUSTED", "Confirmed-unsent request retry limit reached", 409
                    )
            known_manifest = await (
                await conn.execute(
                    "SELECT * FROM public.bot_model_request_manifests WHERE bot_id=%s AND model_request_id=%s",
                    (self.store.bot_id, prepared.request_id),
                )
            ).fetchone()
            if known_manifest:
                if (
                    known_manifest["manifest_digest"] != digest_json(manifest)
                    or known_manifest["logical_step_id"] != step["logical_step_id"]
                ):
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Model semantic request is immutable", 409
                    )
            else:
                manifest_ref = await self.payloads.put_in(
                    conn,
                    "model.manifest",
                    prepared.request_id,
                    manifest,
                    parents=await self.payloads.step_parents_in(conn, step["logical_step_id"]),
                )
                await conn.execute(
                    "INSERT INTO public.bot_model_request_manifests(bot_id,model_request_id,logical_step_id,manifest_payload_id,manifest_digest) VALUES (%s,%s,%s,%s,%s)",
                    (
                        self.store.bot_id,
                        prepared.request_id,
                        step["logical_step_id"],
                        manifest_ref,
                        digest_json(manifest),
                    ),
                )
            day = (await self.store._now(conn)).astimezone(UTC).date()
            await conn.execute(
                "INSERT INTO public.bot_agent_budget_days(bot_id,budget_day) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (self.store.bot_id, day),
            )
            budget = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_budget_days WHERE bot_id=%s AND budget_day=%s FOR UPDATE",
                    (self.store.bot_id, day),
                )
            ).fetchone()
            cap = min(
                values.get("daily_token_budget", 1000000),
                current.get("daily_token_budget", 1000000),
            )
            if (
                budget["reserved_tokens"] + budget["charged_tokens"] + prepared.reserved_tokens
                > cap
            ):
                raise ForestError(
                    "DAILY_TOKEN_BUDGET_EXCEEDED",
                    "Bot daily token capacity is reserved or consumed",
                    409,
                )
            from forest.storage.tree_budget import reserve_in

            await reserve_in(
                conn,
                self.store.bot_id,
                run_id,
                prepared.reserved_tokens,
                min(
                    values.get("run_token_budget", 1000000),
                    current.get("run_token_budget", 1000000),
                ),
            )
            metadata = {
                "estimated_input_tokens": prepared.estimated_input_tokens,
                "max_output_tokens": prepared.max_output_tokens,
                "stream": prepared.stream,
                "serializer_revision": prepared.serializer_revision,
            }
            request_ref = await self.payloads.put_in(
                conn,
                "model.request",
                prepared.transport_attempt_id,
                prepared.body,
                parents=(
                    known_manifest["manifest_payload_id"] if known_manifest else manifest_ref,
                ),
                binary=True,
            )
            attempt = await (
                await conn.execute(
                    "INSERT INTO public.bot_model_transport_attempts(bot_id,transport_attempt_id,operation_id,model_request_id,run_id,logical_step_id,"
                    "model_call_epoch,owner_epoch,actor_epoch,host_incarnation,wire_digest,request_payload_id,prepared_metadata,deadline_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,LEAST(%s,clock_timestamp()+%s*interval '1 second')) RETURNING *",
                    (
                        self.store.bot_id,
                        prepared.transport_attempt_id,
                        operation_id,
                        prepared.request_id,
                        run_id,
                        step["logical_step_id"],
                        step["model_call_epoch"] + 1,
                        head["owner_epoch"],
                        actor.actor_epoch,
                        actor.host.incarnation,
                        prepared.payload_digest,
                        request_ref,
                        Jsonb(metadata),
                        head["deadline_at"],
                        values.get("model_timeout", 60),
                    ),
                )
            ).fetchone()
            await conn.execute(
                "INSERT INTO public.bot_model_budget_reservations(bot_id,transport_attempt_id,budget_day,reserved_tokens) VALUES (%s,%s,%s,%s)",
                (self.store.bot_id, prepared.transport_attempt_id, day, prepared.reserved_tokens),
            )
            await conn.execute(
                "UPDATE public.bot_agent_budget_days SET reserved_tokens=reserved_tokens+%s WHERE bot_id=%s AND budget_day=%s",
                (prepared.reserved_tokens, self.store.bot_id, day),
            )
            await conn.execute(
                "UPDATE public.bot_agent_steps SET model_call_epoch=%s,model_request_id=%s,transport_attempt_id=%s,status='MODEL_PENDING' WHERE bot_id=%s AND logical_step_id=%s",
                (
                    attempt["model_call_epoch"],
                    prepared.request_id,
                    prepared.transport_attempt_id,
                    self.store.bot_id,
                    step["logical_step_id"],
                ),
            )
            await self._checkpoint(conn, head, phase="MODEL_PENDING")
            await self.messages._actor_final(conn, actor, binding)
            if attempt["deadline_at"] <= await self.store._now(conn):
                raise ForestError(
                    "MODEL_DEADLINE_EXCEEDED", "Model deadline expired during admission", 409
                )
            return self._permit(attempt)

    async def _current_attempt(self, conn, actor, permit):
        if permit.bot_id != self.store.bot_id:
            raise ForestError("AGENT_SCOPE_MISMATCH", "Model permit belongs to another Bot", 403)
        binding, conversation, session, run, head = await self._guard(conn, actor, permit.run_id)
        attempt = await (
            await conn.execute(
                "SELECT * FROM public.bot_model_transport_attempts WHERE bot_id=%s AND transport_attempt_id=%s FOR UPDATE",
                (self.store.bot_id, permit.transport_attempt_id),
            )
        ).fetchone()
        step = await (
            await conn.execute(
                "SELECT * FROM public.bot_agent_steps WHERE bot_id=%s AND logical_step_id=%s FOR UPDATE",
                (self.store.bot_id, permit.logical_step_id),
            )
        ).fetchone()
        if (
            attempt is None
            or step is None
            or any(attempt[key] != getattr(permit, key) for key in ModelPermit.__dataclass_fields__)
            or head["owner_epoch"] != permit.owner_epoch
            or head["current_step_id"] != permit.logical_step_id
            or step["transport_attempt_id"] != permit.transport_attempt_id
            or step["model_call_epoch"] != permit.model_call_epoch
        ):
            raise ForestError(
                "MODEL_ATTEMPT_STALE", "Model response no longer owns the current step", 409
            )
        return binding, head, step, attempt

    async def check_model_permit(self, actor, permit, prepared):
        prepared.verify()
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, head, _, attempt = await self._current_attempt(conn, actor, permit)
            await self._configuration(conn, actor, head)
            await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, head["run_id"])
            await self.payloads.require_in(conn, (attempt["request_payload_id"],))
            if (
                attempt["status"] != "PERMITTED"
                or attempt["wire_digest"] != prepared.payload_digest
                or attempt["model_request_id"] != prepared.request_id
                or attempt["transport_attempt_id"] != prepared.transport_attempt_id
            ):
                raise ForestError(
                    "MODEL_DISPATCH_FORBIDDEN",
                    "Model permit is spent or names different bytes",
                    409,
                )
            await conn.execute(
                "UPDATE public.bot_model_transport_attempts SET status='DISPATCHING' WHERE bot_id=%s AND transport_attempt_id=%s",
                (self.store.bot_id, permit.transport_attempt_id),
            )
            await self.messages._actor_final(conn, actor, binding)
            if attempt["deadline_at"] <= await self.store._now(conn):
                raise ForestError("MODEL_DEADLINE_EXCEEDED", "Model dispatch deadline expired", 409)

    async def _settle(self, conn, attempt_id, *, status, usage):
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_model_budget_reservations WHERE bot_id=%s AND transport_attempt_id=%s FOR UPDATE",
                (self.store.bot_id, attempt_id),
            )
        ).fetchone()
        if row["status"] == "SETTLED":
            return
        if status == "UNKNOWN":
            await conn.execute(
                "UPDATE public.bot_model_budget_reservations SET status='UNKNOWN' WHERE bot_id=%s AND transport_attempt_id=%s",
                (self.store.bot_id, attempt_id),
            )
            return
        charged = 0 if status == "NOT_SENT" else row["reserved_tokens"]
        if (
            status == "RESPONDED"
            and usage
            and any(k in usage for k in ("total_tokens", "input_tokens", "prompt_tokens"))
        ):
            charged = usage.get(
                "total_tokens",
                usage.get("input_tokens", usage.get("prompt_tokens", 0))
                + usage.get("output_tokens", usage.get("completion_tokens", 0)),
            )
            if type(charged) is not int or charged < 0:
                raise ForestError(
                    "MODEL_USAGE_INVALID",
                    "Provider usage must contain nonnegative integer tokens",
                    422,
                )
        from forest.storage.tree_budget import settle_in

        owner = await (
            await conn.execute(
                "SELECT run_id FROM public.bot_model_transport_attempts WHERE bot_id=%s AND transport_attempt_id=%s",
                (self.store.bot_id, attempt_id),
            )
        ).fetchone()
        await settle_in(conn, self.store.bot_id, owner["run_id"], row["reserved_tokens"], charged)
        await conn.execute(
            "UPDATE public.bot_agent_budget_days SET reserved_tokens=reserved_tokens-%s,charged_tokens=charged_tokens+%s WHERE bot_id=%s AND budget_day=%s",
            (row["reserved_tokens"], charged, self.store.bot_id, row["budget_day"]),
        )
        await conn.execute(
            "UPDATE public.bot_model_budget_reservations SET status='SETTLED',charged_tokens=%s WHERE bot_id=%s AND transport_attempt_id=%s",
            (charged, self.store.bot_id, attempt_id),
        )

    async def complete_model(self, actor, permit, result):
        data = _json(
            result.as_dict() if hasattr(result, "as_dict") else result, maximum=4 * 1024 * 1024
        )
        observation_digest = digest_json(data)
        if data.get("status") not in {"RESPONDED", "NOT_SENT", "UNKNOWN"}:
            raise ForestError("MODEL_RESULT_INVALID", "Unknown provider result status", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, head, step, attempt = await self._current_attempt(conn, actor, permit)
            if any(
                data.get(key) != expected
                for key, expected in (
                    ("request_id", permit.model_request_id),
                    ("transport_attempt_id", permit.transport_attempt_id),
                    ("payload_digest", permit.wire_digest),
                )
            ):
                raise ForestError(
                    "MODEL_ATTEMPT_STALE", "Provider result names different request bytes", 409
                )
            if attempt["result_digest"] is not None:
                if attempt["result_digest"] != observation_digest:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT",
                        "Model observation already has a different result",
                        409,
                    )
                await self.messages._actor_final(conn, actor, binding)
                return await self._expanded(conn, head)
            late_revoked = (attempt["status"] == "UNKNOWN" and head["phase"] == "BLOCKED_UNKNOWN"
                            and data["status"] == "RESPONDED")
            if not late_revoked and (
                (attempt["status"] != "DISPATCHING"
                 and not (attempt["status"] == "PERMITTED" and data["status"] == "NOT_SENT"))
                or head["phase"] != "MODEL_PENDING"
            ):
                raise ForestError(
                    "MODEL_ATTEMPT_STALE", "This model call cannot publish another result", 409
                )
            try:
                await self._configuration(conn, actor, head)
                await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, head["run_id"])
                if late_revoked:
                    raise ForestError("MODEL_ATTEMPT_STALE", "Known late usage requires a revoked original input", 409)
            except ForestError as exc:
                if exc.code not in {"AGENT_MEMORY_REVOKED", "AGENT_CONFIGURATION_REVOKED", "CONTROL_CONTENT_REVOKED"}:
                    raise
                # A known bill remains known after source revocation. Retain
                # only identity, cost and digest; no returned text/tool body.
                observation = {
                    k: data.get(k)
                    for k in (
                        "status",
                        "request_id",
                        "transport_attempt_id",
                        "payload_digest",
                        "usage",
                    )
                }
                # The frozen 031 observation shape names content revocation;
                # the Run checkpoint separately retains the precise policy code.
                observation["discarded_reason"] = "AGENT_MEMORY_REVOKED"
                observation["usage"] = usage_metadata(data.get("usage"))
                await conn.execute(
                    "UPDATE public.bot_model_transport_attempts SET status=%s,result_metadata=%s,result_digest=%s WHERE bot_id=%s AND transport_attempt_id=%s",
                    (
                        data["status"],
                        Jsonb(observation),
                        digest_json(data),
                        self.store.bot_id,
                        permit.transport_attempt_id,
                    ),
                )
                await self._settle(
                    conn,
                    permit.transport_attempt_id,
                    status=data["status"],
                    usage=observation["usage"],
                )
                await self._abandon(conn, head)
                await conn.execute(
                    "UPDATE public.bot_agent_steps SET status='FAILED' WHERE bot_id=%s AND logical_step_id=%s",
                    (self.store.bot_id, step["logical_step_id"]),
                )
                unknown = await self._has_unknown_in(conn, head["run_id"])
                if not unknown:
                    await self.messages._end_run_in(conn, actor, run_id=head["run_id"], reason=exc.code)
                head = await self._checkpoint(conn, head, phase="BLOCKED_UNKNOWN" if unknown else "FAILED", error_code=exc.code)
                await self.messages._actor_final(conn, actor, binding)
                return await self._expanded(conn, head)
            step = await self.payloads.hydrate_step_in(conn, step)
            turn = None
            protocol_error = None
            if data["status"] == "RESPONDED":
                raw_candidate = data.get("text")
                legacy_wire = (
                    attempt["prepared_metadata"].get("serializer_revision")
                    == "chat-completions-json-v1"
                )
                if legacy_wire:
                    calls = data.get("tool_calls") or []
                    allowed = frozenset(
                        tool.get("name", "") for tool in head["spec"].get("tools", [])
                    )
                    if (
                        not isinstance(raw_candidate, str)
                        or not isinstance(calls, list)
                        or len(calls) > 1
                        or any(not isinstance(call, dict) for call in calls)
                    ):
                        protocol_error = ForestError(
                            "AGENT_PROTOCOL_LEGACY_RESULT",
                            "Legacy in-flight result cannot be projected into one ModelTurn",
                            422,
                        )
                    elif calls:
                        call = calls[0]
                        try:
                            name = call["name"]
                            arguments = call["arguments"]
                            arguments_json = json.dumps(
                                arguments,
                                ensure_ascii=False,
                                sort_keys=True,
                                separators=(",", ":"),
                                allow_nan=False,
                            )
                        except (KeyError, TypeError, ValueError, UnicodeError, RecursionError):
                            protocol_error = ForestError(
                                "AGENT_PROTOCOL_LEGACY_RESULT",
                                "Legacy in-flight tool result is malformed",
                                422,
                            )
                        else:
                            if name not in allowed or name == "forest_analyze":
                                protocol_error = ForestError(
                                    "AGENT_PROTOCOL_LEGACY_RESULT",
                                    "Legacy in-flight tool is unavailable after protocol cutover",
                                    422,
                                )
                            else:
                                blocks = (
                                    *((ThinkingBlock(raw_candidate),) if raw_candidate.strip() else ()),
                                    FunctionCallBlock(name, arguments_json),
                                )
                                turn = ModelTurn(
                                    blocks,
                                    "sha256:"
                                    + hashlib.sha256(raw_candidate.encode("utf-8")).hexdigest(),
                                )
                    elif raw_candidate.strip():
                        turn = ModelTurn(
                            (AssistantBlock(raw_candidate),),
                            "sha256:"
                            + hashlib.sha256(raw_candidate.encode("utf-8")).hexdigest(),
                        )
                    else:
                        protocol_error = ForestError(
                            "AGENT_PROTOCOL_LEGACY_RESULT",
                            "Legacy in-flight result contains no terminal text",
                            422,
                        )
                elif data.get("tool_calls"):
                    protocol_error = ForestError(
                        "AGENT_PROTOCOL_NATIVE_TOOL_CALL",
                        "Provider-native tool calls are not valid Forest Agent output",
                        422,
                    )
                if not legacy_wire and protocol_error is None:
                    values = self._values(head)
                    try:
                        turn = parse_model_turn(
                            raw_candidate,
                            allowed_tools=frozenset(
                                tool.get("name", "")
                                for tool in head["spec"].get("tools", [])
                            ),
                            candidate_byte_limit=values.get(
                                "protocol_candidate_byte_limit", 1024 * 1024
                            ),
                        )
                    except ForestError as exc:
                        protocol_error = exc
                if turn is not None:
                    call = turn.function_call
                    data = {
                        **data,
                        "raw_candidate": raw_candidate,
                        "canonical_model_turn": serialize_model_turn(turn),
                        "model_turn": turn.as_dict(),
                        "text": turn.assistant_text,
                        "tool_calls": []
                        if call is None
                        else [
                            {
                                "id": "call-" + str(step["step_seq"] - 1),
                                "name": call.name,
                                "arguments": call.arguments,
                            }
                        ],
                    }
            result_ref = await self.payloads.put_in(
                conn,
                "model.result",
                permit.transport_attempt_id,
                data,
                parents=(attempt["request_payload_id"],),
            )
            await conn.execute(
                "UPDATE public.bot_model_transport_attempts SET status=%s,result_payload_id=%s,result_digest=%s WHERE bot_id=%s AND transport_attempt_id=%s",
                (
                    data["status"],
                    result_ref,
                    observation_digest,
                    self.store.bot_id,
                    permit.transport_attempt_id,
                ),
            )
            await self._settle(
                conn, permit.transport_attempt_id, status=data["status"], usage=data.get("usage")
            )
            if data["status"] == "RESPONDED" and attempt["deadline_at"] <= await self.store._now(
                conn
            ):
                # A late observation is still a known bill and auditable body,
                # but its syntax is irrelevant: it cannot repair, publish text,
                # or create an executable tool proposal after the frozen call
                # deadline.
                await self._abandon(conn, head)
                await self._flush(conn, head)
                await conn.execute(
                    "UPDATE public.bot_agent_steps SET status='FAILED',output_payload_id=%s,"
                    "output_digest=%s WHERE bot_id=%s AND logical_step_id=%s",
                    (
                        result_ref,
                        digest_json(data),
                        self.store.bot_id,
                        step["logical_step_id"],
                    ),
                )
                await conn.execute(
                    "UPDATE public.bot_agent_runs SET final_step_id=NULL,final_input_revision=NULL "
                    "WHERE bot_id=%s AND run_id=%s",
                    (self.store.bot_id, head["run_id"]),
                )
                await self.messages._end_run_in(
                    conn, actor, run_id=head["run_id"], reason="MODEL_DEADLINE_EXCEEDED"
                )
                head = await self._checkpoint(
                    conn, head, phase="FAILED", error_code="MODEL_DEADLINE_EXCEEDED"
                )
                await self.messages._actor_final(conn, actor, binding)
                return await self._expanded(conn, head)
            if protocol_error is not None:
                head = await self._protocol_failure_in(
                    conn,
                    actor,
                    head,
                    step,
                    result_ref,
                    data,
                    protocol_error,
                )
                await self.messages._actor_final(conn, actor, binding)
                return await self._expanded(conn, head)
            if data["status"] == "RESPONDED" and turn is not None:
                values = self._values(head)
                if turn.function_call is not None:
                    if head["tool_iteration_count"] >= values.get("max_tool_iterations", 8):
                        head = await self._known_model_failure_in(
                            conn, actor, head, step, result_ref, data,
                            "AGENT_TOOL_ITERATION_LIMIT",
                        )
                        await self.messages._actor_final(conn, actor, binding)
                        return await self._expanded(conn, head)
                    await conn.execute(
                        "UPDATE public.bot_agent_runs SET tool_iteration_count=tool_iteration_count+1 "
                        "WHERE bot_id=%s AND run_id=%s",
                        (self.store.bot_id, head["run_id"]),
                    )
                elif turn.thinking_only:
                    if head["thinking_iteration_count"] >= values.get(
                        "max_thinking_iterations", 4
                    ):
                        head = await self._known_model_failure_in(
                            conn, actor, head, step, result_ref, data,
                            "AGENT_THINKING_ITERATION_LIMIT",
                        )
                        await self.messages._actor_final(conn, actor, binding)
                        return await self._expanded(conn, head)
                    await conn.execute(
                        "UPDATE public.bot_agent_runs "
                        "SET thinking_iteration_count=thinking_iteration_count+1 "
                        "WHERE bot_id=%s AND run_id=%s",
                        (self.store.bot_id, head["run_id"]),
                    )
            if data["status"] != "RESPONDED":
                phase = "BLOCKED_UNKNOWN" if data["status"] == "UNKNOWN" else "RETRY_MODEL"
                if phase == "RETRY_MODEL":
                    await conn.execute(
                        "UPDATE public.bot_agent_steps SET status='RETRY_MODEL' WHERE bot_id=%s AND logical_step_id=%s",
                        (self.store.bot_id, step["logical_step_id"]),
                    )
            else:
                calls = data.get("tool_calls", [])
                if (
                    not isinstance(calls, list)
                    or len(calls) > 16
                    or any(not isinstance(call, dict) for call in calls)
                    or len({call.get("id") for call in calls}) != len(calls)
                ):
                    raise ForestError(
                        "MODEL_TOOLS_INVALID", "Tool group must have bounded unique call IDs", 422
                    )
                for index, call in enumerate(calls):
                    _text(call.get("id"), "tool_call_id", 128)
                    _text(call.get("name"), "tool_name", 128)
                    if not isinstance(call.get("arguments"), dict):
                        raise ForestError(
                            "MODEL_TOOLS_INVALID", "Tool arguments must be a JSON object", 422
                        )
                    operation_id = "agent-tool:" + digest_json(
                        [self.store.bot_id, head["run_id"], step["logical_step_id"], index]
                    )
                    arguments_ref = await self.payloads.put_in(
                        conn,
                        "agent.tool",
                        "arguments:" + operation_id,
                        call["arguments"],
                        parents=(result_ref,),
                    )
                    await conn.execute(
                        "INSERT INTO public.bot_agent_tools(bot_id,run_id,logical_step_id,tool_call_id,call_index,tool_name,arguments_payload_id,arguments_digest,operation_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (
                            self.store.bot_id,
                            head["run_id"],
                            step["logical_step_id"],
                            call["id"],
                            index,
                            call["name"],
                            arguments_ref,
                            digest_json(call["arguments"]),
                            operation_id,
                        ),
                    )
                phase = "WAIT_TOOL" if calls else "FINAL_READY"
                if not calls and turn is not None and not turn.terminal:
                    initial = await self.messages.bindings.initial_in(
                        conn, binding, required=False
                    )
                    initial_parents = (
                        (initial["initial_payload_id"],)
                        if initial["display_name_payload_state"] == "ACTIVE"
                        else ()
                    )
                    source = step["snapshot"]["conversation"]
                    author = {
                        "external_user_id": binding["self_id"],
                        "display_name": initial["display_name"]
                        if initial_parents
                        else binding["self_id"],
                        "is_self": True,
                        "identity_origin": "agent_output",
                    }
                    closed_parts = [
                        {
                            "unit_id": step["logical_step_id"] + ":model-turn",
                            "role": "assistant",
                            "source": source,
                            "author": author,
                            "parts": [
                                {"type": "text", "text": data["canonical_model_turn"]}
                            ],
                            "model_turn": data["model_turn"],
                        }
                    ]
                    closed_ref = await self.payloads.put_in(
                        conn,
                        "context.parts",
                        step["logical_step_id"],
                        closed_parts,
                        parents=(
                            *initial_parents,
                            result_ref,
                            *await self.payloads.step_parents_in(
                                conn, step["logical_step_id"], include_output=False
                            ),
                        ),
                    )
                    phase = "READY_MODEL"
                    await conn.execute(
                        "UPDATE public.bot_agent_steps SET status='CLOSED',closed_parts_payload_id=%s "
                        "WHERE bot_id=%s AND logical_step_id=%s",
                        (closed_ref, self.store.bot_id, step["logical_step_id"]),
                    )
                elif not calls and (
                    not isinstance(data.get("text"), str) or not data["text"].strip()
                ):
                    raise ForestError(
                        "MODEL_EMPTY_FINAL",
                        "Model returned no complete final text or tool group",
                        422,
                    )
                await conn.execute(
                    "UPDATE public.bot_agent_steps SET status=CASE WHEN status='CLOSED' THEN status ELSE %s END,"
                    "output_payload_id=%s,output_digest=%s WHERE bot_id=%s AND logical_step_id=%s",
                    (phase, result_ref, digest_json(data), self.store.bot_id, step["logical_step_id"]),
                )
                if not calls and turn is not None and turn.terminal:
                    await conn.execute(
                        "UPDATE public.bot_agent_runs SET final_step_id=%s,final_input_revision=%s WHERE bot_id=%s AND run_id=%s",
                        (
                            step["logical_step_id"],
                            step["input_revision"],
                            self.store.bot_id,
                            head["run_id"],
                        ),
                    )
            head = await self._checkpoint(conn, head, phase=phase)
            await self.messages._actor_final(conn, actor, binding)
            return await self._expanded(conn, head)

    async def _known_model_failure_in(self, conn, actor, head, step, result_ref, data, code):
        await conn.execute(
            "UPDATE public.bot_agent_steps SET status='FAILED',output_payload_id=%s,output_digest=%s "
            "WHERE bot_id=%s AND logical_step_id=%s",
            (result_ref, digest_json(data), self.store.bot_id, step["logical_step_id"]),
        )
        await self._flush(conn, head)
        await self.messages._end_run_in(conn, actor, run_id=head["run_id"], reason=code)
        return await self._checkpoint(conn, head, phase="FAILED", error_code=code)

    async def _protocol_failure_in(
        self, conn, actor, head, step, result_ref, data, error: ForestError
    ):
        """Persist an invalid candidate, then create at most one repair step."""
        values = self._values(head)
        snapshot = step["snapshot"]
        prior_repairs = snapshot.get("protocol_repair", {}).get("repair_count", 0)
        await conn.execute(
            "UPDATE public.bot_agent_steps SET status='FAILED',output_payload_id=%s,output_digest=%s "
            "WHERE bot_id=%s AND logical_step_id=%s",
            (result_ref, digest_json(data), self.store.bot_id, step["logical_step_id"]),
        )
        can_repair = (
            prior_repairs < values.get("max_protocol_repairs", 1)
            and head["next_step_seq"] <= values.get("max_steps", 16)
            and head["deadline_at"] > await self.store._now(conn)
        )
        if not can_repair:
            await self._flush(conn, head)
            await self.messages._end_run_in(
                conn, actor, run_id=head["run_id"], reason="AGENT_PROTOCOL_FAILED"
            )
            return await self._checkpoint(
                conn, head, phase="FAILED", error_code="AGENT_PROTOCOL_FAILED"
            )

        await conn.execute(
            "UPDATE public.bot_agent_runs SET protocol_repair_count=protocol_repair_count+1 "
            "WHERE bot_id=%s AND run_id=%s",
            (self.store.bot_id, head["run_id"]),
        )

        raw = data.get("text") if isinstance(data.get("text"), str) else ""
        limit = values.get("protocol_repair_feedback_byte_limit", 8192)
        raw_bytes = raw.encode("utf-8")
        if len(raw_bytes) > limit:
            half = max(1, limit // 2)
            prefix = raw_bytes[:half].decode("utf-8", errors="ignore")
            suffix = raw_bytes[-half:].decode("utf-8", errors="ignore")
            feedback = prefix + "\n...[candidate truncated by host]...\n" + suffix
            while len(feedback.encode("utf-8")) > limit:
                feedback = feedback[:-1]
        else:
            feedback = raw
        step_id = "model-step:" + digest_json(
            [self.store.bot_id, head["run_id"], head["next_step_seq"]]
        )
        repair_snapshot = {
            **snapshot,
            "logical_step_id": step_id,
            "protocol_repair": {
                "repair_count": prior_repairs + 1,
                "error_code": error.code,
                "candidate_digest": "sha256:" + hashlib.sha256(raw_bytes).hexdigest(),
                "feedback": feedback,
            },
        }
        repair_snapshot = _json(repair_snapshot, maximum=4 * 1024 * 1024)
        snapshot_ref = await self.payloads.put_in(
            conn,
            "model.snapshot",
            step_id,
            repair_snapshot,
            parents=(head["spec_payload_id"], step["snapshot_payload_id"], result_ref),
        )
        await conn.execute(
            "INSERT INTO public.bot_agent_steps(bot_id,run_id,logical_step_id,step_seq,input_revision,"
            "selected_input_end_seq,snapshot_payload_id,snapshot_digest) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                self.store.bot_id,
                head["run_id"],
                step_id,
                head["next_step_seq"],
                head["input_revision"],
                head["selected_input_end_seq"],
                snapshot_ref,
                digest_json(repair_snapshot),
            ),
        )
        await conn.execute(
            "UPDATE public.bot_agent_runs SET current_step_id=%s,next_step_seq=next_step_seq+1 "
            "WHERE bot_id=%s AND run_id=%s",
            (step_id, self.store.bot_id, head["run_id"]),
        )
        return await self._checkpoint(conn, head, phase="PREPARING_MODEL")

    @staticmethod
    def _tool_settings(head, current, name):
        frozen = next(
            (
                tool
                for tool in AgentStore._values(head).get("allowed_tools", [])
                if tool["name"] == name
            ),
            None,
        )
        latest = next(
            (tool for tool in current.get("allowed_tools", []) if tool["name"] == name), None
        )
        if (
            frozen is None
            or latest is None
            or any(
                frozen.get(key) != latest.get(key)
                for key in ("component_id", "service", "contract_major", "effect")
            )
        ):
            raise ForestError(
                "AGENT_TOOL_REVOKED",
                "Tool target is absent or changed in current authorization",
                403,
            )
        definition = next(
            (tool for tool in head["spec"].get("tools", []) if tool["name"] == name), {}
        )
        return {
            **definition,
            **frozen,
            "authority": "binding_admin"
            if "binding_admin" in {frozen.get("authority"), latest.get("authority")}
            else "trigger",
            "approval": "always"
            if "always" in {frozen.get("approval"), latest.get("approval")}
            else "never",
        }

    async def _tool_phase(self, conn, head):
        rows = await (
            await conn.execute(
                "SELECT status FROM public.bot_agent_tools WHERE bot_id=%s AND logical_step_id=%s",
                (self.store.bot_id, head["current_step_id"]),
            )
        ).fetchall()
        states = {r["status"] for r in rows}
        return (
            "BLOCKED_UNKNOWN"
            if "UNKNOWN" in states
            else "WAIT_APPROVAL"
            if "WAIT_APPROVAL" in states
            else "WAIT_TOOL"
        )

    async def _tool(self, conn, run_id, tool_call_id):
        row = await (
            await conn.execute(
                "SELECT *,operation_id AS tool_id FROM public.bot_agent_tools WHERE bot_id=%s AND run_id=%s AND tool_call_id=%s FOR UPDATE",
                (self.store.bot_id, run_id, tool_call_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError(
                "AGENT_TOOL_MISSING", "Tool proposal does not belong to this Run", 404
            )
        return await self.payloads.hydrate_tool_in(conn, row, required=False)

    async def permit_tool(
        self, actor, run_id, tool_call_id, *, authority_trigger_id, requires_approval=False
    ):
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _, _, _, head = await self._guard(conn, actor, run_id)
            current = await self._configuration(conn, actor, head)
            binding["config"] = head["_binding_config"]
            tool = await self._tool(conn, run_id, tool_call_id)
            await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, run_id)
            settings = self._tool_settings(head, current, tool["tool_name"])
            if head["current_step_id"] != tool["logical_step_id"] or head["phase"] not in {
                "WAIT_TOOL",
                "WAIT_APPROVAL",
            }:
                raise ForestError(
                    "AGENT_TOOL_STALE", "This tool group is not the current continuation", 409
                )
            if tool["status"] not in {"PROPOSED", "WAIT_APPROVAL", "NOT_SENT"}:
                raise ForestError(
                    "AGENT_TOOL_DISPATCH_FORBIDDEN",
                    "Tool has already been dispatched or terminated",
                    409,
                )
            trigger = await (
                await conn.execute(
                    "SELECT t.*,i.author FROM public.bot_conversation_triggers t JOIN public.bot_message_inbox i ON i.bot_id=t.bot_id AND i.event_id=t.event_id "
                    "WHERE t.bot_id=%s AND t.trigger_id=%s AND t.claimed_run_id=%s AND t.status='CLAIMED'",
                    (self.store.bot_id, authority_trigger_id, run_id),
                )
            ).fetchone()
            if (
                trigger is None
                or tool["arguments"].get("authority_trigger_id") != authority_trigger_id
            ):
                raise ForestError(
                    "TOOL_AUTHORITY_INVALID",
                    "Tool must name one included trigger, not combined group authority",
                    403,
                )
            if trigger["author"].get("identity_origin") not in {
                "adapter_verified",
                "adapter_anonymous",
            }:
                raise ForestError(
                    "TOOL_AUTHORITY_DENIED", "System triggers do not delegate tool authority", 403
                )
            if settings["authority"] == "binding_admin" and (
                trigger["author"].get("identity_origin") != "adapter_verified"
                or trigger["author"].get("anonymous") is not None
                or trigger["author"].get("is_self")
                or trigger["author"]["external_user_id"]
                not in binding["config"].get("admin_user_ids", [])
            ):
                raise ForestError(
                    "TOOL_AUTHORITY_DENIED",
                    "This trigger author is not a configured Binding administrator",
                    403,
                )
            if tool["authority_trigger_id"] is not None and (
                tool["authority_trigger_id"] != authority_trigger_id
                or tool["principal_ref"] != trigger["principal_ref"]
            ):
                raise ForestError(
                    "TOOL_AUTHORITY_INVALID",
                    "Tool source authority is immutable after selection",
                    403,
                )
            required = (
                requires_approval or tool["requires_approval"] or settings["approval"] == "always"
            )
            await conn.execute(
                "UPDATE public.bot_agent_tools SET authority_trigger_id=%s,principal_ref=%s,requires_approval=%s WHERE bot_id=%s AND run_id=%s AND tool_call_id=%s",
                (
                    authority_trigger_id,
                    trigger["principal_ref"],
                    required,
                    self.store.bot_id,
                    run_id,
                    tool_call_id,
                ),
            )
            if required:
                approval = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_agent_tool_approvals WHERE bot_id=%s AND run_id=%s AND tool_call_id=%s FOR UPDATE",
                        (self.store.bot_id, run_id, tool_call_id),
                    )
                ).fetchone()
                if approval is None:
                    approval = await (
                        await conn.execute(
                            "INSERT INTO public.bot_agent_tool_approvals(bot_id,approval_id,run_id,tool_call_id,arguments_digest,authority_trigger_id,principal_ref,policy_revision,expires_at) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,LEAST(%s,clock_timestamp()+interval '10 minutes')) RETURNING *",
                            (
                                self.store.bot_id,
                                str(uuid4()),
                                run_id,
                                tool_call_id,
                                tool["arguments_digest"],
                                authority_trigger_id,
                                trigger["principal_ref"],
                                actor.policy_revision,
                                head["deadline_at"],
                            ),
                        )
                    ).fetchone()
                if (
                    approval["expires_at"] <= await self.store._now(conn)
                    or approval["decision"] == "REJECT"
                ):
                    raise ForestError(
                        "TOOL_APPROVAL_REJECTED", "Tool approval expired or was rejected", 403
                    )
                if approval["decision"] is None:
                    await conn.execute(
                        "UPDATE public.bot_agent_tools SET status='WAIT_APPROVAL' WHERE bot_id=%s AND run_id=%s AND tool_call_id=%s",
                        (self.store.bot_id, run_id, tool_call_id),
                    )
                    head = await self._checkpoint(conn, head, phase="WAIT_APPROVAL")
                    await self.messages._actor_final(conn, actor, binding)
                    return {
                        "status": "WAIT_APPROVAL",
                        "approval_id": approval["approval_id"],
                        "run_id": run_id,
                        "tool_call_id": tool_call_id,
                    }
                if (
                    approval["policy_revision"] != actor.policy_revision
                    or approval["arguments_digest"] != tool["arguments_digest"]
                ):
                    raise ForestError(
                        "TOOL_APPROVAL_STALE",
                        "Approval no longer authorizes these exact parameters",
                        403,
                    )
            if tool["attempt_count"] >= 3:
                raise ForestError(
                    "TOOL_RETRY_EXHAUSTED", "Confirmed-unsent tool retry limit reached", 409
                )
            tool = await (
                await conn.execute(
                    "UPDATE public.bot_agent_tools SET status='DISPATCHING',attempt_count=attempt_count+1,owner_epoch=%s,actor_epoch=%s,host_incarnation=%s "
                    "WHERE bot_id=%s AND run_id=%s AND tool_call_id=%s RETURNING *",
                    (
                        head["owner_epoch"],
                        actor.actor_epoch,
                        actor.host.incarnation,
                        self.store.bot_id,
                        run_id,
                        tool_call_id,
                    ),
                )
            ).fetchone()
            await self._checkpoint(conn, head, phase="WAIT_TOOL")
            await self.messages._actor_final(conn, actor, binding)
            if head["deadline_at"] <= await self.store._now(conn):
                raise ForestError(
                    "AGENT_BUDGET_EXCEEDED", "Run deadline elapsed before tool dispatch", 409
                )
            return ToolPermit(**{key: tool[key] for key in ToolPermit.__dataclass_fields__})

    async def _kernel_tool_receipt_in(self, conn, tool):
        receipt = await (
            await conn.execute(
                "SELECT * FROM public.bot_calls WHERE bot_id=%s AND operation_id=%s AND status='COMPLETED'",
                (self.store.bot_id, tool["operation_id"]),
            )
        ).fetchone()
        if receipt is None:
            return None
        if receipt.get("agent_tool_id") != tool["operation_id"]:
            raise ForestError("TOOL_RESULT_CONFLICT", "Kernel receipt belongs to another tool", 409)
        return await KernelPayloadStore(self.store).hydrate_call_in(
            conn, receipt, epoch=await self.payloads.epoch_in(conn)
        )

    async def _guard_result_scope_in(self, conn, conversation, head, payload_ids):
        from forest.storage.payload_scope import guard_payload_scope_in

        try:
            await guard_payload_scope_in(
                conn, self.store, conversation, head["selected_input_end_seq"], payload_ids,
                legacy_run_id=head["run_id"],
            )
        except ForestError as exc:
            if exc.code != "PAYLOAD_SCOPE_DENIED":
                raise
            raise ForestError("AGENT_MEMORY_REVOKED", "Tool content is unavailable in this conversation", 409) from None

    async def observe_tool(
        self, actor, permit: ToolPermit, *, status, result, result_parent_ids=()
    ):
        result = _json(result, maximum=4 * 1024 * 1024)
        if not isinstance(result_parent_ids, (tuple, list)) or len(result_parent_ids) > 1:
            raise ForestError("TOOL_RESULT_INVALID", "Tool result has one Kernel receipt", 422)
        result_parent_ids = tuple(_text(value, "result payload ID") for value in result_parent_ids)
        if result_parent_ids and status != "SUCCEEDED":
            raise ForestError("TOOL_RESULT_INVALID", "Only completed results have a Kernel receipt", 422)
        if status not in {"SUCCEEDED", "FAILED", "NOT_SENT", "UNKNOWN"}:
            raise ForestError(
                "TOOL_RESULT_INVALID", "Tool result must have a defined effect outcome", 422
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, conversation, _, _, head = await self._guard(conn, actor, permit.run_id)
            tool = await self._tool(conn, permit.run_id, permit.tool_call_id)
            if (
                any(tool[key] != getattr(permit, key) for key in ToolPermit.__dataclass_fields__)
                or head["owner_epoch"] != permit.owner_epoch
            ):
                raise ForestError(
                    "AGENT_TOOL_STALE", "Tool result belongs to another execution owner", 409
                )
            digest = digest_json([status, result])
            if tool["status"] == status and tool["result_digest"] == digest:
                await self.messages._actor_final(conn, actor, binding)
                return await self._expanded(conn, head)
            if tool["status"] != "DISPATCHING":
                raise ForestError(
                    "AGENT_TOOL_STALE", "Tool result was already settled or blocked", 409
                )
            revoked_code = None
            try:
                current = await self._configuration(conn, actor, head)
                self._tool_settings(head, current, tool["tool_name"])
            except ForestError as exc:
                if exc.code not in {
                    "AGENT_MEMORY_REVOKED",
                    "AGENT_CONFIGURATION_REVOKED",
                    "CONTROL_CONTENT_REVOKED",
                }:
                    raise
                revoked_code = exc.code
            if status == "SUCCEEDED":
                receipt = await self._kernel_tool_receipt_in(conn, tool)
                if (receipt is not None and (
                    result_parent_ids != (receipt["result_payload_id"],)
                    or digest_json(receipt["result"]) != digest_json(result)
                )) or (receipt is None and result_parent_ids):
                    raise ForestError(
                        "TOOL_RESULT_CONFLICT", "Tool result differs from its Kernel receipt", 409
                    )
                # A completed but out-of-scope effect must remain an unknown
                # observation.  Let the runner enter reconciliation instead of
                # converting this publication failure into a clean Run failure.
                await self._guard_result_scope_in(
                    conn, conversation, head, result_parent_ids
                )
            try:
                if revoked_code is not None:
                    raise ForestError(revoked_code, "Tool authority content is unavailable", 409)
                await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, permit.run_id)
                result_ref = await self.payloads.tool_result_in(
                    conn, tool, result, status=status, parents=result_parent_ids
                )
            except ForestError as exc:
                if exc.code not in {
                    "AGENT_MEMORY_REVOKED",
                    "AGENT_CONFIGURATION_REVOKED",
                    "CONTROL_CONTENT_REVOKED",
                }:
                    raise
                revoked_code, result_ref = exc.code, None
            await conn.execute(
                "UPDATE public.bot_agent_tools SET status=%s,result_payload_id=%s,result_digest=%s,result_metadata=%s WHERE bot_id=%s AND run_id=%s AND tool_call_id=%s",
                (
                    status,
                    result_ref,
                    digest,
                    Jsonb(
                        {"discarded_reason": "AGENT_MEMORY_REVOKED"}
                        if revoked_code is not None
                        else {}
                    ),
                    self.store.bot_id,
                    permit.run_id,
                    permit.tool_call_id,
                ),
            )
            if revoked_code is not None:
                await self._abandon(conn, head)
                await self.messages._end_run_in(
                    conn, actor, run_id=permit.run_id, reason=revoked_code
                )
                head = await self._checkpoint(
                    conn, head, phase="FAILED", error_code=revoked_code
                )
                await self.messages._actor_final(conn, actor, binding)
                return await self._expanded(conn, head)
            head = await self._checkpoint(conn, head, phase=await self._tool_phase(conn, head))
            await self.messages._actor_final(conn, actor, binding)
            return await self._expanded(conn, head)

    async def recover_tool(self, actor, run_id, tool_call_id):
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, conversation, _, _, head = await self._guard(conn, actor, run_id)
            tool = await self._tool(conn, run_id, tool_call_id)
            if tool["status"] == "UNKNOWN":
                await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, run_id)
                receipt = await self._kernel_tool_receipt_in(conn, tool)
                if receipt is not None:
                    await self._guard_result_scope_in(
                        conn, conversation, head, (receipt["result_payload_id"],)
                    )
                    # Recover an observation only. Original tool ownership may
                    # precede this Actor, but current source authority, approval
                    # and configuration must still permit accepting its result.
                    await self.guard_tool_call_in(
                        conn, self.store.bot_id, tool["operation_id"], recovery=True,
                        input_digest=receipt["input_digest"],
                    )
                    result_ref = await self.payloads.tool_result_in(
                        conn, tool, receipt["result"], status="SUCCEEDED",
                        parents=(receipt["result_payload_id"],),
                    )
                    await conn.execute(
                        "UPDATE public.bot_agent_tools SET status='SUCCEEDED',result_payload_id=%s,result_digest=%s WHERE bot_id=%s AND run_id=%s AND tool_call_id=%s",
                        (
                            result_ref,
                            digest_json(["SUCCEEDED", receipt["result"]]),
                            self.store.bot_id,
                            run_id,
                            tool_call_id,
                        ),
                    )
                head = await self._checkpoint(conn, head, phase=await self._tool_phase(conn, head))
            await self.messages._actor_final(conn, actor, binding)
            return await self._expanded(conn, head)

    async def close_tools(self, actor, run_id):
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, conversation, _, _, head = await self._guard(conn, actor, run_id)
            await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, run_id)
            tools = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_tools WHERE bot_id=%s AND logical_step_id=%s ORDER BY call_index FOR UPDATE",
                    (self.store.bot_id, head["current_step_id"]),
                )
            ).fetchall()
            tools = [await self.payloads.hydrate_tool_in(conn, tool) for tool in tools]
            if not tools or any(
                tool["status"] not in {"SUCCEEDED", "FAILED", "CANCELLED"} for tool in tools
            ):
                raise ForestError(
                    "TOOL_GROUP_OPEN",
                    "Every tool must have a definitive terminal record before the next input",
                    409,
                )
            step = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_steps WHERE bot_id=%s AND logical_step_id=%s FOR UPDATE",
                    (self.store.bot_id, head["current_step_id"]),
                )
            ).fetchone()
            if step["status"] == "CLOSED":
                await self.messages._actor_final(conn, actor, binding)
                return await self._expanded(conn, head)
            step = await self.payloads.hydrate_step_in(conn, step)
            initial = await self.messages.bindings.initial_in(conn, binding, required=False)
            initial_parents = (initial["initial_payload_id"],) if initial["display_name_payload_state"] == "ACTIVE" else ()
            source = {
                key: conversation[key]
                for key in (
                    "conversation_id",
                    "group_source_id",
                    "platform",
                    "channel_kind",
                    "channel_id",
                    "thread_id",
                )
            }
            author = {
                "external_user_id": binding["self_id"],
                "display_name": initial["display_name"] if initial_parents else binding["self_id"],
                "is_self": True,
                "identity_origin": "agent_output",
            }
            parts = [
                {
                    "unit_id": step["logical_step_id"] + ":assistant",
                    "role": "assistant",
                    "source": source,
                    "author": author,
                    "parts": [{"type": "text", "text": step["output"].get(
                        "canonical_model_turn", step["output"].get("text", "")
                    )}],
                    "tool_calls": step["output"]["tool_calls"],
                    **({"model_turn": step["output"]["model_turn"]}
                       if "model_turn" in step["output"] else {}),
                }
            ]
            for tool in tools:
                result = tool["result"]
                content = (
                    result.get("parts")
                    if isinstance(result, dict) and isinstance(result.get("parts"), list)
                    else [{"type": "json", "value": result}]
                )
                parts.append(
                    {
                        "unit_id": step["logical_step_id"] + ":tool:" + str(tool["call_index"]),
                        "role": "tool",
                        "source": source,
                        "author": author,
                        "parts": content,
                        "tool_call_id": tool["tool_call_id"],
                        "tool_status": tool["status"],
                        "authority_trigger_id": tool["authority_trigger_id"],
                        "principal_ref": tool["principal_ref"],
                    }
                )
            closed_ref = await self.payloads.put_in(
                conn,
                "context.parts",
                step["logical_step_id"],
                parts,
                parents=(
                    *initial_parents,
                    *await self.payloads.step_parents_in(
                        conn, step["logical_step_id"], include_output=True
                    ),
                    *[t["result_payload_id"] for t in tools if t["result_payload_id"]],
                ),
            )
            await conn.execute(
                "UPDATE public.bot_agent_steps SET status='CLOSED',closed_parts_payload_id=%s WHERE bot_id=%s AND logical_step_id=%s",
                (closed_ref, self.store.bot_id, step["logical_step_id"]),
            )
            head = await self._checkpoint(conn, head, phase="READY_MODEL")
            await self.messages._actor_final(conn, actor, binding)
            return await self._expanded(conn, head)

    async def commit_final(self, actor, run_id, operation_id, delivery):
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _, _, _, head = await self._guard(conn, actor, run_id, terminal=True)
            await self._configuration(conn, actor, head)
            if head["phase"] == "COMPLETED":
                reply = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_conversation_replies WHERE bot_id=%s AND reply_id=%s",
                        (self.store.bot_id, head["final_reply_id"]),
                    )
                ).fetchone()
                from forest.storage.context_payloads import ContextPayloadStore

                reply = await ContextPayloadStore(self.store).read_reply_in(
                    conn,
                    reply,
                    expected_deletion_epoch=await self.payloads.epoch_in(conn),
                    required=False,
                )
                await self.messages._actor_final(conn, actor, binding)
                return reply
            if (
                head["phase"] != "FINAL_READY"
                or head["input_revision"] != head["final_input_revision"]
            ):
                raise ForestError(
                    "AGENT_FINAL_NOT_READY",
                    "Only the frozen FINAL_READY checkpoint can publish",
                    409,
                )
            step = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_steps WHERE bot_id=%s AND logical_step_id=%s FOR UPDATE",
                    (self.store.bot_id, head["final_step_id"]),
                )
            ).fetchone()
            if (
                step["status"] != "FINAL_READY"
                or step["input_revision"] != head["final_input_revision"]
            ):
                raise ForestError(
                    "AGENT_FINAL_NOT_READY", "Final candidate no longer matches its model step", 409
                )
            await self._flush(conn, head)
            step = await self.payloads.hydrate_step_in(conn, step)
            reply = await self.messages._finalize_reply_in(
                conn,
                actor,
                operation_id,
                run_id=run_id,
                expected_input_revision=head["final_input_revision"],
                reply_parts=[[{"type": "text", "text": step["output"]["text"]}]],
                delivery=delivery,
            )
            await conn.execute(
                "UPDATE public.bot_agent_steps SET status='FINAL_COMMITTED',published_context_revision=%s WHERE bot_id=%s AND logical_step_id=%s",
                (
                    (await self.messages._conversation(conn, actor.conversation_id))[
                        "context_revision"
                    ],
                    self.store.bot_id,
                    step["logical_step_id"],
                ),
            )
            await conn.execute(
                "UPDATE public.bot_agent_runs SET final_reply_id=%s WHERE bot_id=%s AND run_id=%s",
                (reply["reply_id"], self.store.bot_id, run_id),
            )
            await self._checkpoint(conn, head, phase="COMPLETED")
            await self.messages._actor_final(conn, actor, binding)
            return reply

    async def _abandon(self, conn, head):
        run_id = head["run_id"]
        await conn.execute(
            "UPDATE public.bot_agent_tools SET status=CASE WHEN status='DISPATCHING' THEN 'UNKNOWN' ELSE 'CANCELLED' END "
            "WHERE bot_id=%s AND run_id=%s AND status IN ('PROPOSED','WAIT_APPROVAL','DISPATCHING','NOT_SENT')",
            (self.store.bot_id, run_id),
        )
        attempts = await (
            await conn.execute(
                "SELECT transport_attempt_id,status FROM public.bot_model_transport_attempts WHERE bot_id=%s AND run_id=%s AND status IN ('PERMITTED','DISPATCHING') FOR UPDATE",
                (self.store.bot_id, run_id),
            )
        ).fetchall()
        for attempt in attempts:
            outcome = "NOT_SENT" if attempt["status"] == "PERMITTED" else "UNKNOWN"
            await conn.execute(
                "UPDATE public.bot_model_transport_attempts SET status=%s WHERE bot_id=%s AND transport_attempt_id=%s",
                (outcome, self.store.bot_id, attempt["transport_attempt_id"]),
            )
            await self._settle(conn, attempt["transport_attempt_id"], status=outcome, usage=None)

    async def _has_unknown_in(self, conn, run_id):
        return bool(await (await conn.execute(
            "SELECT 1 FROM public.bot_model_transport_attempts WHERE bot_id=%s AND run_id=%s "
            "AND status IN ('DISPATCHING','UNKNOWN') UNION ALL "
            "SELECT 1 FROM public.bot_agent_tools WHERE bot_id=%s AND run_id=%s "
            "AND status IN ('DISPATCHING','UNKNOWN') LIMIT 1",
            (self.store.bot_id, run_id, self.store.bot_id, run_id),
        )).fetchone())

    async def fail_revoked(self, actor, run_id):
        """End a removed-content checkpoint before its old owner can hydrate it.

        Only the live Actor of this exact active Session can use this recovery.
        The exception from an earlier read is not proof: recheck revocation in
        this transaction, while preserving every prior attempt/effect identity.
        """
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _ = await self.messages._actor(conn, actor)
            head = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_runs WHERE bot_id=%s AND run_id=%s",
                    (self.store.bot_id, run_id),
                )
            ).fetchone()
            if head is None:
                await self.messages._actor_final(conn, actor, binding)
                return None
            if head["conversation_id"] != actor.conversation_id:
                raise ForestError(
                    "AGENT_SCOPE_MISMATCH", "Run belongs to another conversation", 403
                )
            if head["phase"] in {"FAILED", "CANCELLED", "COMPLETED"}:
                await self.messages._actor_final(conn, actor, binding)
                return head
            # owner=False does not waive the current Actor, active Session,
            # input revision, binding, Host, policy or storage-generation fence.
            binding, _, _, _, head = await self._guard(conn, actor, run_id, owner=False)
            try:
                await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, run_id)
            except ForestError as exc:
                if exc.code != "AGENT_MEMORY_REVOKED":
                    raise
            else:
                raise ForestError(
                    "AGENT_CONTENT_NOT_REVOKED", "Current Agent dependencies remain authorized", 409
                )
            # Never flush a removed private group just to terminate its Run.
            await self._abandon(conn, head)
            unknown = await self._has_unknown_in(conn, run_id)
            if not unknown:
                await self.messages._end_run_in(
                    conn, actor, run_id=run_id, reason="AGENT_MEMORY_REVOKED"
                )
            head = await self._checkpoint(
                conn, head, phase="BLOCKED_UNKNOWN" if unknown else "FAILED", error_code="AGENT_MEMORY_REVOKED"
            )
            await self.messages._actor_final(conn, actor, binding)
            return head

    async def fail(self, actor, run_id, error_code, *, status="FAILED", delivery=None):
        _text(error_code, "error_code", 256)
        if status not in {"FAILED", "CANCELLED"}:
            raise ValueError("status")
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _, _, _, head = await self._guard(conn, actor, run_id, terminal=True)
            if head["phase"] in {"FAILED", "CANCELLED", "COMPLETED"}:
                await self.messages._actor_final(conn, actor, binding)
                return head
            try:
                await self._flush(conn, head)
            except ForestError as exc:
                if exc.code != "AGENT_MEMORY_REVOKED":
                    raise
                # Termination must remain possible after evidence revocation;
                # unflushed derived context cannot be admitted by this path.
                error_code = exc.code
            await self._abandon(conn, head)
            if status == "FAILED" and await self._has_unknown_in(conn, run_id):
                head = await self._checkpoint(conn, head, phase="BLOCKED_UNKNOWN", error_code=error_code)
                await self.messages._actor_final(conn, actor, binding)
                return head
            await self.messages._end_run_in(
                conn, actor, run_id=run_id, reason=error_code, status=status, delivery=delivery
            )
            head = await self._checkpoint(conn, head, phase=status, error_code=error_code)
            await self.messages._actor_final(conn, actor, binding)
            return head

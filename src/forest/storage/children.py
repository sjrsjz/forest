"""Private, depth-one analysis; only a current parent Actor adopts its result."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC
from types import SimpleNamespace
from uuid import uuid4

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.models import PreparedModelRequest
from forest.packages import digest_json
from forest.storage import tree_budget
from forest.storage.agent_config import AgentConfig
from forest.storage.agent_memory import AgentMemoryStore
from forest.storage.child_payloads import ChildPayloadStore, result_metadata, spec_metadata
from forest.storage.messages import _json

TOOL_NAME = "forest_analyze"


def declaration(snapshot):
    return {
        "name": TOOL_NAME,
        "description": "Analyze a fixed subset of current evidence privately; the parent alone replies. No tools or child delegation.",
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "required": ["authority_trigger_id", "arguments"],
            "properties": {
                "authority_trigger_id": {"type": "string", "enum": snapshot.get("trigger_ids", [])},
                "arguments": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["task", "source_unit_ids"],
                    "properties": {
                        "task": {"type": "string", "minLength": 1, "maxLength": 4000},
                        "source_unit_ids": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 64,
                            "uniqueItems": True,
                            "items": {
                                "type": "string",
                                "enum": [u["unit_id"] for u in snapshot.get("units", [])],
                            },
                        },
                    },
                },
            },
        },
    }


@dataclass(frozen=True)
class ChildLease:
    host: object
    child_id: str
    owner_id: str
    lease_epoch: int
    expires_at: object
    policy_revision: int
    maintenance_epoch: int
    storage_generation: int


class ChildStore:
    def __init__(self, agent):
        self.agent, self.messages, self.store = agent, agent.messages, agent.store
        self.payloads = ChildPayloadStore(self.store)

    async def _row(self, conn, child_id):
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_agent_children WHERE bot_id=%s AND child_id=%s FOR UPDATE",
                (self.store.bot_id, child_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("CHILD_NOT_FOUND", "Private child does not belong to this Bot", 404)
        return row

    async def spawn(self, actor, run_id):
        """Reserve every private proposal in this causal group in one transaction."""
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _, _, _, head = await self.agent._guard(conn, actor, run_id)
            await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, run_id)
            current = await self.agent._configuration(conn, actor, head)
            values = self.agent._values(head)
            if head["phase"] != "WAIT_TOOL":
                return await self.agent._expanded(conn, head)
            proposals = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_tools WHERE bot_id=%s AND logical_step_id=%s AND tool_name=%s AND status='PROPOSED' ORDER BY call_index FOR UPDATE",
                    (self.store.bot_id, head["current_step_id"], TOOL_NAME),
                )
            ).fetchall()
            if not proposals:
                return await self.agent._expanded(conn, head)
            if not values.get("children_enabled", False) or not current.get(
                "children_enabled", False
            ):
                raise ForestError("CHILD_DISABLED", "Private analysis is not authorized", 403)
            step = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_steps WHERE bot_id=%s AND logical_step_id=%s",
                    (self.store.bot_id, head["current_step_id"]),
                )
            ).fetchone()
            step = await self.agent.payloads.hydrate_step_in(conn, step)
            proposals = [await self.agent.payloads.hydrate_tool_in(conn, p) for p in proposals]
            snapshot = step["snapshot"]
            count = (
                await (
                    await conn.execute(
                        "SELECT count(*) AS n FROM public.bot_agent_children WHERE bot_id=%s AND parent_run_id=%s",
                        (self.store.bot_id, run_id),
                    )
                ).fetchone()
            )["n"]
            if count + len(proposals) > min(
                values.get("max_children", 4), current.get("max_children", 4)
            ):
                raise ForestError(
                    "CHILD_LIMIT_EXCEEDED", "This Run has no remaining child allocation", 409
                )
            from jsonschema import Draft202012Validator

            validator = Draft202012Validator(declaration(snapshot)["parameters"])
            for proposal in proposals:
                wrapper = proposal["arguments"]
                if not validator.is_valid(wrapper):
                    raise ForestError(
                        "CHILD_INPUT_INVALID",
                        "Child task must select exact source units and one trigger",
                        422,
                    )
                trigger = await (
                    await conn.execute(
                        "SELECT t.*,i.author FROM public.bot_conversation_triggers t JOIN public.bot_message_inbox i USING(bot_id,event_id) WHERE t.bot_id=%s AND t.trigger_id=%s AND t.claimed_run_id=%s AND t.status='CLAIMED'",
                        (self.store.bot_id, wrapper["authority_trigger_id"], run_id),
                    )
                ).fetchone()
                if (
                    not trigger
                    or trigger["trigger_id"] not in snapshot["trigger_ids"]
                    or trigger["author"].get("identity_origin")
                    not in {"adapter_verified", "adapter_anonymous"}
                ):
                    raise ForestError(
                        "CHILD_AUTHORITY_INVALID",
                        "Child authority is not an included user trigger",
                        403,
                    )
                child_id = "child:" + digest_json([self.store.bot_id, proposal["operation_id"]])
                selected = set(wrapper["arguments"]["source_unit_ids"])
                units = [deepcopy(u) for u in snapshot["units"] if u["unit_id"] in selected]
                # A child has no tools: parent tool facts remain typed evidence,
                # never assistant/tool protocol messages requiring tool capability.
                for unit in units:
                    if unit["role"] in {"assistant", "tool"}:
                        unit["parts"] = [
                            {
                                "type": "json",
                                "value": {
                                    "role": unit["role"],
                                    "parts": unit["parts"],
                                    "tool_calls": unit.get("tool_calls", []),
                                    "tool_call_id": unit.get("tool_call_id"),
                                },
                            }
                        ]
                        unit["role"] = "user"
                        unit.pop("tool_calls", None)
                        unit.pop("tool_call_id", None)
                units.append(
                    {
                        "unit_id": child_id + ":task",
                        "role": "user",
                        "source": snapshot["conversation"],
                        "author": {
                            "principal_ref": trigger["principal_ref"],
                            "identity_origin": "host_child_task",
                        },
                        "parts": [{"type": "text", "text": wrapper["arguments"]["task"]}],
                    }
                )
                frozen = {
                    **{
                        k: deepcopy(snapshot[k])
                        for k in (
                            "bot_id",
                            "input_revision",
                            "selected_input_end_seq",
                            "context_revision",
                            "context_view_revision",
                            "input_batch_ids",
                            "conversation",
                        )
                    },
                    "run_id": child_id,
                    "logical_step_id": child_id + ":analysis",
                    "parent_run_id": run_id,
                    "trigger_ids": [trigger["trigger_id"]],
                    "units": units,
                    "required_unit_ids": [u["unit_id"] for u in units],
                }
                spec = {
                    "config_revision": head["spec"].get("config_revision", 0),
                    "values": deepcopy(values),
                    "depth": 1,
                    "delegation_policy_revision": actor.policy_revision,
                }
                snapshot_id, spec_id = await self.payloads.spawn_in(
                    conn,
                    child_id,
                    step["logical_step_id"],
                    proposal,
                    _json(frozen, maximum=4 * 1024 * 1024),
                    spec,
                )
                await conn.execute(
                    "INSERT INTO public.bot_agent_children(bot_id,child_id,parent_run_id,parent_step_id,tool_call_id,parent_input_revision,parent_snapshot_digest,authority_trigger_id,principal_ref,source_unit_ids,snapshot_payload_id,snapshot_digest,spec_payload_id,spec_metadata,deadline_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,LEAST(%s,clock_timestamp()+%s*interval '1 second'))",
                    (
                        self.store.bot_id,
                        child_id,
                        run_id,
                        step["logical_step_id"],
                        proposal["tool_call_id"],
                        head["input_revision"],
                        step["snapshot_digest"],
                        trigger["trigger_id"],
                        trigger["principal_ref"],
                        Jsonb(
                            [u["unit_id"] for u in snapshot["units"] if u["unit_id"] in selected]
                        ),
                        snapshot_id,
                        digest_json(frozen),
                        spec_id,
                        Jsonb(spec_metadata(spec)),
                        head["deadline_at"],
                        min(values.get("child_timeout", 90), current.get("child_timeout", 90)),
                    ),
                )
                await conn.execute(
                    "UPDATE public.bot_agent_tools SET status='WAIT_CHILD',authority_trigger_id=%s,principal_ref=%s WHERE bot_id=%s AND run_id=%s AND tool_call_id=%s",
                    (
                        trigger["trigger_id"],
                        trigger["principal_ref"],
                        self.store.bot_id,
                        run_id,
                        proposal["tool_call_id"],
                    ),
                )
            head = await self.agent._checkpoint(conn, head, phase="WAIT_TOOL")
            await self.messages._actor_final(conn, actor, binding)
            return await self.agent._expanded(conn, head)

    async def _scope(self, conn, host, child_id):
        identity = await (
            await conn.execute(
                "SELECT c.binding_id,c.conversation_id FROM public.bot_agent_children x JOIN public.bot_agent_runs a ON a.bot_id=x.bot_id AND a.run_id=x.parent_run_id JOIN public.bot_conversations c ON c.bot_id=a.bot_id AND c.conversation_id=a.conversation_id WHERE x.bot_id=%s AND x.child_id=%s",
                (self.store.bot_id, child_id),
            )
        ).fetchone()
        if not identity:
            raise ForestError("CHILD_NOT_FOUND", "Child is unavailable", 404)
        binding = await self.messages.bindings._guard(conn, host, identity["binding_id"])
        conversation = await self.messages._conversation(conn, identity["conversation_id"])
        session = await self.messages._session(conn, conversation)
        row = await self._row(conn, child_id)
        head = await self.agent._head(conn, row["parent_run_id"])
        runtime = binding["_runtime"]
        if (
            not session
            or session["active_run_id"] != row["parent_run_id"]
            or head["current_step_id"] != row["parent_step_id"]
            or head["input_revision"] != row["parent_input_revision"]
            or head["phase"] not in {"WAIT_TOOL", "WAIT_APPROVAL", "BLOCKED_UNKNOWN"}
            or row["spec_metadata"]["delegation_policy_revision"] != runtime["policy_revision"]
        ):
            raise ForestError(
                "CHILD_PARENT_REVOKED",
                "Parent no longer owns this private analysis delegation",
                409,
            )
        current = row["spec_metadata"]["values"]
        if row["spec_metadata"]["config_revision"]:
            cfg = await AgentConfig.guard_in(
                conn,
                SimpleNamespace(host=host, policy_revision=runtime["policy_revision"]),
                row["spec_metadata"]["config_revision"],
            )
            current = cfg["current_values"]
        if not current.get("children_enabled", False):
            raise ForestError("CHILD_DISABLED", "Current policy disabled private analysis", 403)
        trigger = await (
            await conn.execute(
                "SELECT 1 FROM public.bot_conversation_triggers WHERE bot_id=%s AND trigger_id=%s AND claimed_run_id=%s AND status='CLAIMED' AND principal_ref=%s",
                (
                    self.store.bot_id,
                    row["authority_trigger_id"],
                    row["parent_run_id"],
                    row["principal_ref"],
                ),
            )
        ).fetchone()
        if not trigger:
            raise ForestError(
                "CHILD_AUTHORITY_INVALID", "Delegating trigger is no longer current", 403
            )
        hidden = await (
            await conn.execute(
                "SELECT 1 FROM public.bot_context_units u JOIN public.bot_message_inbox i ON i.bot_id=u.bot_id AND i.event_id=u.event_id "
                "WHERE u.bot_id=%s AND u.unit_id=ANY(%s) AND i.context_disposition<>'READABLE' LIMIT 1",
                (self.store.bot_id, row["source_unit_ids"]),
            )
        ).fetchone()
        if hidden:
            raise ForestError(
                "CHILD_SOURCE_REVOKED", "Selected parent evidence is no longer readable", 403
            )
        return binding, head, row, current

    async def _guard(self, conn, lease, *, terminal=False):
        binding, head, row, current = await self._scope(conn, lease.host, lease.child_id)
        if not terminal:
            await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, row["parent_run_id"])
            await self.payloads.guard_in(conn, row)
        if (
            row["owner_id"] != lease.owner_id
            or row["lease_epoch"] != lease.lease_epoch
            or row["host_incarnation"] != lease.host.incarnation
            or row["host_owner_epoch"] != lease.host.owner_epoch
            or any(
                binding["_runtime"][k] != getattr(lease, k)
                for k in ("policy_revision", "maintenance_epoch", "storage_generation")
            )
            or not row["lease_until"]
            or row["lease_until"] <= await self.store._now(conn)
            or (not terminal and row["status"] not in {"RUNNING", "RETRY"})
        ):
            raise ForestError("STALE_CHILD", "Child execution lease expired or was replaced", 409)
        return binding, head, row, current

    async def _final(self, conn, lease, binding):
        await self.messages.bindings._final_guard(conn, lease.host, binding)
        row = await self._row(conn, lease.child_id)
        if row["lease_until"] <= await self.store._now(conn):
            raise ForestError("STALE_CHILD", "Child lease expired before commit", 409)

    async def pending(self, host, limit=32):
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, host)
            return await (
                await conn.execute(
                    "SELECT child_id FROM public.bot_agent_children WHERE bot_id=%s AND (status IN ('QUEUED','RETRY') OR (status='RUNNING' AND (lease_until<=clock_timestamp() OR host_incarnation<>%s))) ORDER BY created_at LIMIT %s",
                    (self.store.bot_id, host.incarnation, limit),
                )
            ).fetchall()

    async def claim(self, host, child_id, owner_id, *, ttl=120):
        self.store._ttl(ttl)
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, head, row, current = await self._scope(conn, host, child_id)
            now = await self.store._now(conn)
            if row["status"] not in {"QUEUED", "RETRY", "RUNNING"}:
                return None
            if (
                row["lease_until"]
                and row["lease_until"] > now
                and row["host_incarnation"] == host.incarnation
            ):
                return None
            active = (
                await (
                    await conn.execute(
                        "SELECT count(*) AS n FROM public.bot_agent_children WHERE bot_id=%s AND status='RUNNING' AND lease_until>clock_timestamp() AND host_incarnation=%s",
                        (self.store.bot_id, host.incarnation),
                    )
                ).fetchone()
            )["n"]
            if active >= min(
                2,
                current.get("child_concurrency", 2),
                row["spec_metadata"]["values"].get("child_concurrency", 2),
            ):
                return None
            if row["attempt_id"]:
                attempt = await self._attempt(conn, row["attempt_id"])
                if attempt["status"] == "DISPATCHING":
                    await conn.execute(
                        "UPDATE public.bot_child_model_attempts SET status='UNKNOWN' WHERE bot_id=%s AND attempt_id=%s",
                        (self.store.bot_id, attempt["attempt_id"]),
                    )
                    await self._unknown(conn, head, row)
                    await self.messages.bindings._final_guard(conn, host, binding)
                    return None
                if attempt["status"] == "PERMITTED":
                    await conn.execute(
                        "UPDATE public.bot_child_model_attempts SET status='NOT_SENT' WHERE bot_id=%s AND attempt_id=%s",
                        (self.store.bot_id, attempt["attempt_id"]),
                    )
                    await self._settle(conn, row, attempt, "NOT_SENT", None)
            if row["deadline_at"] <= now:
                await self._failed(conn, row, "CHILD_DEADLINE_EXCEEDED")
                await self.messages.bindings._final_guard(conn, host, binding)
                return None
            row = await (
                await conn.execute(
                    "UPDATE public.bot_agent_children SET status='RUNNING',owner_id=%s,lease_epoch=lease_epoch+1,lease_until=clock_timestamp()+%s*interval '1 second',host_incarnation=%s,host_owner_epoch=%s,policy_revision=%s,maintenance_epoch=%s,storage_generation=%s WHERE bot_id=%s AND child_id=%s RETURNING *",
                    (
                        owner_id,
                        ttl,
                        host.incarnation,
                        host.owner_epoch,
                        binding["_runtime"]["policy_revision"],
                        binding["_runtime"]["maintenance_epoch"],
                        binding["_runtime"]["storage_generation"],
                        self.store.bot_id,
                        child_id,
                    ),
                )
            ).fetchone()
            await self.messages.bindings._final_guard(conn, host, binding)
            return ChildLease(
                host,
                child_id,
                owner_id,
                row["lease_epoch"],
                row["lease_until"],
                row["policy_revision"],
                row["maintenance_epoch"],
                row["storage_generation"],
            )

    async def load(self, lease):
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _, row, current = await self._guard(conn, lease)
            row = await self.payloads.hydrate_in(conn, row)
            row["current_values"] = current
            row["attempt"] = (
                await self.payloads.hydrate_attempt_in(
                    conn, await self._attempt(conn, row["attempt_id"])
                )
                if row["attempt_id"]
                else None
            )
            await self._final(conn, lease, binding)
            return row

    async def renew(self, lease, ttl=120):
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _, _, _ = await self._guard(conn, lease)
            await self._final(conn, lease, binding)
            await conn.execute(
                "UPDATE public.bot_agent_children SET lease_until=clock_timestamp()+%s*interval '1 second' WHERE bot_id=%s AND child_id=%s",
                (ttl, self.store.bot_id, lease.child_id),
            )

    async def _attempt(self, conn, attempt_id):
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_child_model_attempts WHERE bot_id=%s AND attempt_id=%s FOR UPDATE",
                (self.store.bot_id, attempt_id),
            )
        ).fetchone()

    @staticmethod
    def replay(row):
        old = row["attempt"]
        if not old or old["status"] != "NOT_SENT":
            raise ForestError(
                "CHILD_RETRY_FORBIDDEN", "Only a confirmed unsent request can retry", 409
            )
        return PreparedModelRequest(
            old["request_id"],
            uuid4().hex,
            bytes(old["request_body"]),
            json.dumps(old["manifest"], sort_keys=True, separators=(",", ":")),
            old["wire_digest"],
            **old["metadata"],
        )

    async def prepare(self, lease, prepared):
        prepared.verify()
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _, row, current = await self._guard(conn, lease)
            previous = await self._attempt(conn, prepared.transport_attempt_id)
            if previous:
                await self.payloads.guard_in(conn, row, previous)
                if (
                    previous["child_id"] != row["child_id"]
                    or previous["wire_digest"] != prepared.payload_digest
                    or previous["lease_epoch"] != lease.lease_epoch
                ):
                    raise ForestError(
                        "CHILD_REQUEST_CONFLICT", "Child attempt identity was reused", 409
                    )
                await self._final(conn, lease, binding)
                return previous["attempt_id"]
            if row["attempt_count"] >= 3:
                raise ForestError(
                    "CHILD_RETRY_EXHAUSTED", "Private request retry limit reached", 409
                )
            if (
                row["deadline_at"] <= await self.store._now(conn)
                or prepared.manifest.get("input_digest") != row["snapshot_digest"]
                or prepared.manifest.get("tool_names")
            ):
                raise ForestError(
                    "CHILD_INPUT_INVALID",
                    "Child request changed its frozen evidence or attempted tools",
                    409,
                )
            values = row["spec_metadata"]["values"]
            if prepared.max_output_tokens > min(
                values.get("child_max_output_tokens", 1024), values.get("max_output_tokens", 1024)
            ):
                raise ForestError(
                    "CHILD_OUTPUT_BUDGET", "Child request exceeds delegated output budget", 409
                )
            if row["attempt_id"]:
                old = await self._attempt(conn, row["attempt_id"])
                old = await self.payloads.hydrate_attempt_in(conn, old)
                if (
                    old["status"] != "NOT_SENT"
                    or old["wire_digest"] != prepared.payload_digest
                    or old["request_id"] != prepared.request_id
                    or old["manifest"] != prepared.manifest
                ):
                    raise ForestError(
                        "CHILD_RETRY_FORBIDDEN",
                        "Child retries preserve exact original bytes and input",
                        409,
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
            if budget["reserved_tokens"] + budget[
                "charged_tokens"
            ] + prepared.reserved_tokens > min(
                values.get("daily_token_budget", 1000000),
                current.get("daily_token_budget", 1000000),
            ):
                raise ForestError(
                    "DAILY_TOKEN_BUDGET_EXCEEDED", "Bot daily token capacity is exhausted", 409
                )
            await tree_budget.reserve_in(
                conn,
                self.store.bot_id,
                row["parent_run_id"],
                prepared.reserved_tokens,
                min(
                    values.get("run_token_budget", 1000000),
                    current.get("run_token_budget", 1000000),
                ),
            )
            await conn.execute(
                "UPDATE public.bot_agent_budget_days SET reserved_tokens=reserved_tokens+%s WHERE bot_id=%s AND budget_day=%s",
                (prepared.reserved_tokens, self.store.bot_id, day),
            )
            metadata = {
                "estimated_input_tokens": prepared.estimated_input_tokens,
                "max_output_tokens": prepared.max_output_tokens,
                "stream": prepared.stream,
                "serializer_revision": prepared.serializer_revision,
            }
            request_id, manifest_id = await self.payloads.request_in(conn, row, prepared)
            await conn.execute(
                "INSERT INTO public.bot_child_model_attempts(bot_id,attempt_id,child_id,request_id,request_payload_id,wire_digest,manifest_payload_id,metadata,owner_id,lease_epoch,host_incarnation,host_owner_epoch,status,budget_day,reserved_tokens,deadline_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'PERMITTED',%s,%s,LEAST(%s,clock_timestamp()+%s*interval '1 second'))",
                (
                    self.store.bot_id,
                    prepared.transport_attempt_id,
                    row["child_id"],
                    prepared.request_id,
                    request_id,
                    prepared.payload_digest,
                    manifest_id,
                    Jsonb(metadata),
                    lease.owner_id,
                    lease.lease_epoch,
                    lease.host.incarnation,
                    lease.host.owner_epoch,
                    day,
                    prepared.reserved_tokens,
                    row["deadline_at"],
                    values.get("model_timeout", 60),
                ),
            )
            await conn.execute(
                "UPDATE public.bot_agent_children SET attempt_id=%s,attempt_count=attempt_count+1 WHERE bot_id=%s AND child_id=%s",
                (prepared.transport_attempt_id, self.store.bot_id, row["child_id"]),
            )
            await self._final(conn, lease, binding)
            return prepared.transport_attempt_id

    async def check_permit(self, lease, attempt_id, prepared):
        prepared.verify()
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _, row, _ = await self._guard(conn, lease)
            attempt = await self._attempt(conn, attempt_id)
            if (
                not attempt
                or row["attempt_id"] != attempt_id
                or attempt["lease_epoch"] != lease.lease_epoch
                or attempt["status"] != "PERMITTED"
                or attempt["wire_digest"] != prepared.payload_digest
                or prepared.transport_attempt_id != attempt_id
                or prepared.request_id != attempt["request_id"]
            ):
                raise ForestError(
                    "CHILD_ATTEMPT_STALE", "Child transport no longer owns this exact request", 409
                )
            await self.payloads.guard_in(conn, row, attempt)
            await conn.execute(
                "UPDATE public.bot_child_model_attempts SET status='DISPATCHING' WHERE bot_id=%s AND attempt_id=%s",
                (self.store.bot_id, attempt_id),
            )
            await self._final(conn, lease, binding)
            if min(attempt["deadline_at"], row["deadline_at"]) <= await self.store._now(conn):
                raise ForestError("CHILD_DEADLINE_EXCEEDED", "Child dispatch deadline elapsed", 409)

    async def _settle(self, conn, row, attempt, status, usage):
        if attempt["settled"] or status == "UNKNOWN":
            return
        charged = 0 if status == "NOT_SENT" else attempt["reserved_tokens"]
        if status == "RESPONDED" and isinstance(usage, dict) and "total_tokens" in usage:
            charged = usage["total_tokens"]
            if type(charged) is not int or charged < 0:
                raise ForestError("MODEL_USAGE_INVALID", "Invalid child token usage", 422)
        await tree_budget.settle_in(
            conn, self.store.bot_id, row["parent_run_id"], attempt["reserved_tokens"], charged
        )
        await conn.execute(
            "UPDATE public.bot_agent_budget_days SET reserved_tokens=reserved_tokens-%s,charged_tokens=charged_tokens+%s WHERE bot_id=%s AND budget_day=%s",
            (attempt["reserved_tokens"], charged, self.store.bot_id, attempt["budget_day"]),
        )
        await conn.execute(
            "UPDATE public.bot_child_model_attempts SET settled=true,charged_tokens=%s WHERE bot_id=%s AND attempt_id=%s",
            (charged, self.store.bot_id, attempt["attempt_id"]),
        )

    async def _wake(self, conn, row):
        await conn.execute(
            "UPDATE public.bot_conversations c SET mailbox_revision=mailbox_revision+1 FROM public.bot_agent_runs a WHERE a.bot_id=c.bot_id AND a.conversation_id=c.conversation_id AND a.bot_id=%s AND a.run_id=%s",
            (self.store.bot_id, row["parent_run_id"]),
        )

    async def _failed(self, conn, row, code):
        await conn.execute(
            "UPDATE public.bot_agent_children SET status='FAILED',error_code=%s WHERE bot_id=%s AND child_id=%s",
            (code, self.store.bot_id, row["child_id"]),
        )
        await self._wake(conn, row)

    async def _unknown(self, conn, head, row):
        await conn.execute(
            "UPDATE public.bot_agent_children SET status='UNKNOWN',error_code='CHILD_MODEL_UNKNOWN' WHERE bot_id=%s AND child_id=%s",
            (self.store.bot_id, row["child_id"]),
        )
        await conn.execute(
            "UPDATE public.bot_agent_tools SET status='UNKNOWN' WHERE bot_id=%s AND run_id=%s AND tool_call_id=%s AND status='WAIT_CHILD'",
            (self.store.bot_id, row["parent_run_id"], row["tool_call_id"]),
        )
        await self.agent._checkpoint(
            conn, head, phase="BLOCKED_UNKNOWN", error_code="CHILD_MODEL_UNKNOWN"
        )
        await self._wake(conn, row)

    async def complete(self, lease, attempt_id, result):
        data = _json(result.as_dict(), maximum=4 * 1024 * 1024)
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, head, row, _ = await self._guard(conn, lease, terminal=True)
            attempt = await self._attempt(conn, attempt_id)
            if (
                not attempt
                or row["attempt_id"] != attempt_id
                or attempt["lease_epoch"] != lease.lease_epoch
                or data["transport_attempt_id"] != attempt_id
                or data["request_id"] != attempt["request_id"]
                or data["payload_digest"] != attempt["wire_digest"]
            ):
                raise ForestError(
                    "CHILD_ATTEMPT_STALE", "Child response is from another attempt", 409
                )
            if attempt["result_digest"]:
                if attempt["result_digest"] != digest_json(data):
                    raise ForestError(
                        "CHILD_RESULT_CONFLICT", "Child observation is immutable", 409
                    )
                await self._final(conn, lease, binding)
                return row
            if row["status"] != "RUNNING" or not (
                attempt["status"] == "DISPATCHING"
                or (attempt["status"] == "PERMITTED" and data["status"] == "NOT_SENT")
            ):
                raise ForestError(
                    "CHILD_ATTEMPT_STALE", "Child attempt cannot accept another result", 409
                )
            try:
                await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, row["parent_run_id"])
                await self.payloads.guard_in(conn, row, attempt)
            except ForestError as exc:
                if exc.code != "AGENT_MEMORY_REVOKED":
                    raise
                metadata = result_metadata(data, discarded_reason=exc.code)
                await conn.execute(
                    "UPDATE public.bot_child_model_attempts SET status=%s,result_metadata=%s,result_digest=%s WHERE bot_id=%s AND attempt_id=%s",
                    (
                        data["status"],
                        Jsonb(metadata),
                        digest_json(data),
                        self.store.bot_id,
                        attempt_id,
                    ),
                )
                await self._settle(conn, row, attempt, data["status"], data.get("usage"))
                await self._failed(conn, row, exc.code)
                await self._final(conn, lease, binding)
                return await self._row(conn, row["child_id"])
            result_id = await self.payloads.observation_in(conn, attempt, data)
            await conn.execute(
                "UPDATE public.bot_child_model_attempts SET status=%s,result_payload_id=%s,result_metadata=%s,result_digest=%s WHERE bot_id=%s AND attempt_id=%s",
                (
                    data["status"],
                    result_id,
                    Jsonb(result_metadata(data)),
                    digest_json(data),
                    self.store.bot_id,
                    attempt_id,
                ),
            )
            await self._settle(conn, row, attempt, data["status"], data.get("usage"))
            if data["status"] == "UNKNOWN":
                await self._unknown(conn, head, row)
            elif data["status"] == "NOT_SENT":
                await conn.execute(
                    "UPDATE public.bot_agent_children SET status='RETRY',lease_until=clock_timestamp() WHERE bot_id=%s AND child_id=%s",
                    (self.store.bot_id, row["child_id"]),
                )
                # The exact bytes can be retried after releasing this worker.
                await self.messages.bindings._final_guard(conn, lease.host, binding)
                return await self._row(conn, row["child_id"])
            elif data.get("tool_calls") or not data.get("text", "").strip():
                await self._failed(conn, row, "CHILD_TOOL_OR_EMPTY_OUTPUT")
            elif min(row["deadline_at"], attempt["deadline_at"]) <= await self.store._now(conn):
                await self._failed(conn, row, "CHILD_DEADLINE_EXCEEDED")
            else:
                row = await self.payloads.hydrate_in(conn, row)
                candidate = {
                    "child_id": row["child_id"],
                    "text": data["text"],
                    "source_unit_ids": row["source_unit_ids"],
                    "source": row["snapshot"]["conversation"],
                    "authority_trigger_id": row["authority_trigger_id"],
                    "parent_input_revision": row["parent_input_revision"],
                }
                candidate_id = await self.payloads.candidate_in(conn, row, candidate, result_id)
                await conn.execute(
                    "UPDATE public.bot_agent_children SET status='RESULT_READY',result_payload_id=%s,result_digest=%s WHERE bot_id=%s AND child_id=%s",
                    (candidate_id, digest_json(candidate), self.store.bot_id, row["child_id"]),
                )
                await self._wake(conn, row)
            await self._final(conn, lease, binding)
            return await self._row(conn, row["child_id"])

    async def adopt(self, actor, run_id):
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _, _, _, head = await self.agent._guard(conn, actor, run_id)
            await AgentMemoryStore.guard_run_in(conn, self.store.bot_id, run_id)
            current = await self.agent._configuration(conn, actor, head)
            if not current.get("children_enabled", False):
                raise ForestError("CHILD_DISABLED", "Private result adoption was revoked", 403)
            rows = await (
                await conn.execute(
                    "SELECT x.* FROM public.bot_agent_children x JOIN public.bot_agent_tools t ON t.bot_id=x.bot_id AND t.run_id=x.parent_run_id AND t.tool_call_id=x.tool_call_id WHERE x.bot_id=%s AND x.parent_step_id=%s AND t.status='WAIT_CHILD' ORDER BY t.call_index FOR UPDATE OF x,t",
                    (self.store.bot_id, head["current_step_id"]),
                )
            ).fetchall()
            for row in rows:
                await self._scope(conn, actor.host, row["child_id"])
                if row["parent_input_revision"] != head["input_revision"]:
                    raise ForestError(
                        "CHILD_PARENT_REVOKED", "Child result belongs to an older parent cut", 409
                    )
                if row["status"] not in {"RESULT_READY", "FAILED", "CANCELLED"}:
                    continue
                await self.payloads.guard_in(conn, row)
                row = await self.payloads.hydrate_in(conn, row)
                result = (
                    row["result"]
                    if row["status"] == "RESULT_READY"
                    else {
                        "child_id": row["child_id"],
                        "error_code": row["error_code"] or row["status"],
                        "source_unit_ids": row["source_unit_ids"],
                    }
                )
                status = "SUCCEEDED" if row["status"] == "RESULT_READY" else "FAILED"
                tool = await self.agent._tool(conn, run_id, row["tool_call_id"])
                result_ref = await self.agent.payloads.tool_result_in(
                    conn,
                    tool,
                    result,
                    status=status,
                    parents=(row["result_payload_id"],) if row.get("result_payload_id") else (),
                )
                await conn.execute(
                    "UPDATE public.bot_agent_tools SET status=%s,result_payload_id=%s,result_digest=%s WHERE bot_id=%s AND run_id=%s AND tool_call_id=%s",
                    (
                        status,
                        result_ref,
                        digest_json([status, result]),
                        self.store.bot_id,
                        run_id,
                        row["tool_call_id"],
                    ),
                )
                await conn.execute(
                    "UPDATE public.bot_agent_children SET status='ACCEPTED',accepted_checkpoint=%s WHERE bot_id=%s AND child_id=%s",
                    (head["checkpoint_seq"] + 1, self.store.bot_id, row["child_id"]),
                )
            if rows:
                head = await self.agent._checkpoint(conn, head)
            await self.messages._actor_final(conn, actor, binding)
            return await self.agent._expanded(conn, head)

    async def cancel_in(self, conn, head):
        rows = await (
            await conn.execute(
                "SELECT * FROM public.bot_agent_children WHERE bot_id=%s AND parent_run_id=%s AND status NOT IN ('CANCELLED','ACCEPTED') FOR UPDATE",
                (self.store.bot_id, head["run_id"]),
            )
        ).fetchall()
        for row in rows:
            if row["attempt_id"]:
                attempt = await self._attempt(conn, row["attempt_id"])
                if attempt["status"] in {"PERMITTED", "DISPATCHING"}:
                    status = "NOT_SENT" if attempt["status"] == "PERMITTED" else "UNKNOWN"
                    await conn.execute(
                        "UPDATE public.bot_child_model_attempts SET status=%s WHERE bot_id=%s AND attempt_id=%s",
                        (status, self.store.bot_id, attempt["attempt_id"]),
                    )
                    await self._settle(conn, row, attempt, status, None)
            await conn.execute(
                "UPDATE public.bot_agent_children SET status='CANCELLED',lease_epoch=lease_epoch+1,error_code='PARENT_CANCELLED' WHERE bot_id=%s AND child_id=%s",
                (self.store.bot_id, row["child_id"]),
            )

    async def reject(self, host, child_id, code, *, lease=None):
        """Persist known preparation failure; a dispatched attempt remains unknown."""
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._runtime(conn)
            await self.store._host(conn, host)
            row = await self._row(conn, child_id)
            if lease is not None and (
                row["owner_id"] != lease.owner_id or row["lease_epoch"] != lease.lease_epoch
            ):
                return
            if (
                lease is None
                and row["status"] == "RUNNING"
                and row["lease_until"]
                and row["lease_until"] > await self.store._now(conn)
            ):
                return
            head = await self.agent._head(conn, row["parent_run_id"])
            if row["status"] in {"RESULT_READY", "UNKNOWN", "FAILED", "CANCELLED", "ACCEPTED"}:
                return
            attempt = await self._attempt(conn, row["attempt_id"]) if row["attempt_id"] else None
            if attempt and attempt["status"] == "DISPATCHING":
                await conn.execute(
                    "UPDATE public.bot_child_model_attempts SET status='UNKNOWN' WHERE bot_id=%s AND attempt_id=%s",
                    (self.store.bot_id, attempt["attempt_id"]),
                )
                await self._unknown(conn, head, row)
            else:
                if attempt and attempt["status"] == "PERMITTED":
                    await conn.execute(
                        "UPDATE public.bot_child_model_attempts SET status='NOT_SENT' WHERE bot_id=%s AND attempt_id=%s",
                        (self.store.bot_id, attempt["attempt_id"]),
                    )
                    await self._settle(conn, row, attempt, "NOT_SENT", None)
                await self._failed(conn, row, code)
            await self.store._host(conn, host)

    async def media(self, lease):
        """Read only validated captures of selected parent evidence, never fetch URLs."""
        from forest.storage.input_payloads import InputContentStore
        from forest.storage.media import MAX_CONTEXT_MEDIA_BYTES, MediaStore

        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _, row, _ = await self._guard(conn, lease)
            row = await self.payloads.hydrate_in(conn, row)
            media, total = {}, 0
            for unit in row["snapshot"]["units"]:
                if unit["unit_id"] not in row["source_unit_ids"] or unit["role"] != "user":
                    continue
                for index, part in enumerate(unit["parts"]):
                    if part.get("type") != "image" or not isinstance(part.get("media_ref"), str):
                        continue
                    capture = await (
                        await conn.execute(
                            "SELECT m.*,u.unit_id,i.ingress_seq,i.conversation_id,i.context_disposition FROM public.bot_media_captures m JOIN public.bot_context_units u ON u.bot_id=m.bot_id AND u.event_id=m.event_id JOIN public.bot_message_inbox i ON i.bot_id=m.bot_id AND i.event_id=m.event_id WHERE m.bot_id=%s AND m.media_ref=%s AND u.unit_id=%s AND m.part_index=%s",
                            (self.store.bot_id, part["media_ref"], unit["unit_id"], index),
                        )
                    ).fetchone()
                    if (
                        capture
                        and capture["context_disposition"] == "READABLE"
                        and capture["conversation_id"]
                        == row["snapshot"]["conversation"]["conversation_id"]
                        and capture["ingress_seq"] <= row["snapshot"]["selected_input_end_seq"]
                    ):
                        body = await InputContentStore(self.store).read_capture_in(
                            conn,
                            part["media_ref"],
                            expected_deletion_epoch=await self.agent.payloads.epoch_in(conn),
                        )
                        if body is None:
                            continue
                        capture = {**capture, **body}
                        total += len(capture["content"])
                        if total > MAX_CONTEXT_MEDIA_BYTES:
                            break
                        media[(unit["unit_id"], str(index))] = MediaStore._snapshot(
                            capture, capture
                        )
            await self._final(conn, lease, binding)
            return media

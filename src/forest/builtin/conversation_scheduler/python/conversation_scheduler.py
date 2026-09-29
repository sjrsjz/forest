import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from forest.errors import ForestError
from forest.sdk import Plugin, job, service


class ConversationScheduler(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        value = dict(config)
        if value.get("protocol") != "forest.conversation.scheduler/2":
            raise ForestError(
                "CONVERSATION_CONFIGURATION_INVALID",
                "Conversation Scheduler configuration is invalid",
                422,
            )
        raw_view = value.get("context_view")
        view = dict(raw_view) if isinstance(raw_view, Mapping) else raw_view
        if (
            not isinstance(view, dict)
            or set(view)
            != {"high_watermark_units", "retain_units", "policy_revision"}
            or type(view["high_watermark_units"]) is not int
            or type(view["retain_units"]) is not int
            or not 2 <= view["high_watermark_units"] <= 1_000_000
            or not 1 <= view["retain_units"] < view["high_watermark_units"]
            or view["policy_revision"] != "context-view-unit-window-v1"
        ):
            raise ForestError(
                "CONVERSATION_CONFIGURATION_INVALID",
                "Conversation Context view configuration is invalid",
                422,
            )
        self._context_view = view

    @staticmethod
    def _exact(reference):
        return {
            name: reference[name]
            for name in (
                "payload_id",
                "digest",
                "owner_component_id",
                "owner_kind",
                "owner_id",
            )
        }

    @service(
        "conversation.context.prefill",
        semantics="idempotent",
        contract="1.0",
        input_schema="schemas/prefill-service-input.json",
        output_schema="schemas/select-input-output.json",
    )
    async def prefill_trigger_context(self, payload, ctx):
        request = payload
        selection = {
            "actor": request["actor"],
            "boundary": request["boundary"],
            "selection_mode": "PREFILL_ONLY",
            "session_ref": request["session_ref"],
            "run_ref": request["run_ref"],
            "next_run_ref": None,
        }
        result = await ctx.uow.call(
            "conversation.prefill-trigger-input",
            {
                "selection": selection,
                "context_view": self._context_view,
                "payload": {
                    "protocol": "forest.payload-json-from-prior/1",
                    "prior_step_id": "prepare",
                },
                "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
                "qualification": {
                    "selection_step_id": "select",
                    "deletion_epoch_step_id": "payload_epoch",
                },
                "memory": {},
            },
            operation_id=ctx.operations.stable("prefill:" + request["request_id"]),
            timeout=30,
        )
        if result["status"] == "BATCH":
            if result["kind"] != "CONTEXT_PREFILL" or len(result["event_ids"]) > 64:
                raise ForestError(
                    "CONVERSATION_PREFILL_RESULT_INVALID",
                    "Context prefill returned an invalid batch",
                    409,
                )
        elif result["status"] != "CONTINUE":
            raise ForestError(
                "CONVERSATION_PREFILL_RESULT_INVALID",
                "Context prefill returned an invalid boundary",
                409,
            )
        return result

    @staticmethod
    def _unit_parents(units):
        parents = {}
        for unit in units:
            payload = {
                "payload_id": unit["payload_id"],
                "digest": unit["payload_digest"],
                "owner_component_id": unit["payload_owner_component_id"],
                "owner_kind": unit["payload_owner_kind"],
                "owner_id": unit["payload_owner_id"],
            }
            parents[payload["payload_id"]] = payload
            if unit["author_payload_id"] is not None:
                author = {
                    "payload_id": unit["author_payload_id"],
                    "digest": unit["author_payload_digest"],
                    "owner_component_id": unit["author_owner_component_id"],
                    "owner_kind": unit["author_owner_kind"],
                    "owner_id": unit["author_owner_id"],
                }
                parents[author["payload_id"]] = author
        return tuple(parents[key] for key in sorted(parents))

    @staticmethod
    def _archive_unit(page):
        archive = page["archive"]
        if archive is None:
            return None
        payload = archive["payload"]
        return {
            "conversation_id": page["conversation_id"],
            "unit_id": "archive:" + archive["compaction_id"].removeprefix("compaction:"),
            "context_revision": archive["first_context_revision"],
            "unit_index": 0,
            "kind": "CONTROL",
            "source_component_id": "conversation.scheduler",
            "source_kind": "context.archive",
            "source_id": archive["compaction_id"],
            "input_batch_id": None,
            "event_id": None,
            "ingress_seq": None,
            "payload_id": payload["payload_id"],
            "payload_digest": payload["digest"],
            "payload_owner_component_id": payload["owner_component_id"],
            "payload_owner_kind": payload["owner_kind"],
            "payload_owner_id": payload["owner_id"],
            "author_payload_id": None,
            "author_payload_digest": None,
            "author_owner_component_id": None,
            "author_owner_kind": None,
            "author_owner_id": None,
            "author_subject_ref": None,
            "trigger_decision": None,
        }

    async def _snapshot_tree(self, pages, ctx):
        nodes = list(pages)
        level = 0
        while len(nodes) > 128:
            next_level = []
            for group_index, start in enumerate(range(0, len(nodes), 128)):
                children = nodes[start : start + 128]
                saved = await ctx.payloads.put_json(
                    {
                        "protocol": "forest.conversation-context-index/1",
                        "level": level,
                        "children": children,
                    },
                    owner_kind="context.snapshot.index",
                    owner_id=ctx.operations.stable(
                        f"context-snapshot-index:{level}:{group_index}"
                    ),
                    parents=children,
                )
                next_level.append(self._exact(saved))
            nodes = next_level
            level += 1
        return nodes, level

    @service("conversation.context.snapshot", semantics="idempotent", contract="2.0")
    async def context_snapshot(self, payload, ctx):
        actor = payload["actor"]
        offset = 0
        metadata = None
        pages = []
        archive_present = False
        while True:
            page = await ctx.queries.call(
                "conversation.context-page",
                {"actor": actor, "offset": offset, "limit": 32},
            )
            current_metadata = {
                key: page[key]
                for key in (
                    "conversation_id",
                    "context_applied_seq",
                    "classified_through_seq",
                    "context_revision",
                    "context_view_revision",
                    "source_component_id",
                    "binding_ref",
                    "platform",
                    "channel_kind",
                    "channel_ref",
                    "thread_ref",
                    "context_view_unit_count",
                    "context_archived_unit_count",
                    "archive",
                    "total_units",
                )
            }
            if metadata is None:
                metadata = current_metadata
            elif current_metadata != metadata:
                raise ForestError(
                    "CONTEXT_SNAPSHOT_FENCE_CHANGED",
                    "Conversation Context changed during snapshot materialization",
                    409,
                )
            units = list(page["units"])
            if offset == 0:
                archive_unit = self._archive_unit(page)
                if archive_unit is not None:
                    units.insert(0, archive_unit)
                    archive_present = True
            if units:
                saved = await ctx.payloads.put_json(
                    {
                        "protocol": "forest.conversation-context-page/1",
                        "conversation_id": page["conversation_id"],
                        "context_revision": page["context_revision"],
                        "context_view_revision": page["context_view_revision"],
                        "offset": offset,
                        "units": units,
                    },
                    owner_kind="context.snapshot.page",
                    owner_id=ctx.operations.stable(f"context-snapshot-page:{offset}"),
                    parents=self._unit_parents(units),
                )
                pages.append(self._exact(saved))
            next_offset = page["next_offset"]
            if next_offset is None:
                break
            if next_offset <= offset:
                raise ForestError(
                    "CONTEXT_SNAPSHOT_PAGE_INVALID",
                    "Conversation Context pagination made no progress",
                    500,
                )
            offset = next_offset
        unit_count = metadata["total_units"] + int(archive_present)
        if metadata["context_view_unit_count"] != metadata["total_units"]:
            raise ForestError(
                "CONTEXT_SNAPSHOT_COUNT_CONFLICT",
                "Conversation Context count differs from its materialized view",
                409,
            )
        roots, tree_depth = await self._snapshot_tree(pages, ctx)
        owner_id = ctx.operations.stable("context-snapshot-manifest:v2")
        document = {
            "protocol": "forest.conversation-context-snapshot/2",
            **metadata,
            "unit_count": unit_count,
            "tree_depth": tree_depth,
            "roots": roots,
        }
        saved = await ctx.payloads.put_json(
            document,
            owner_kind="context.snapshot",
            owner_id=owner_id,
            parents=roots,
        )
        return {
            "protocol": "forest.conversation-context-snapshot/2",
            "conversation_id": metadata["conversation_id"],
            "context_applied_seq": metadata["context_applied_seq"],
            "context_revision": metadata["context_revision"],
            "context_view_revision": metadata["context_view_revision"],
            "unit_count": unit_count,
            "snapshot": self._exact(saved),
        }

    @job("conversation.silent-drain")
    async def silent_drain(self, task, ctx):
        result = await ctx.uow.call(
            "conversation.commit-silent-input",
            {
                "selection": task["payload"],
                "context_view": self._context_view,
                "payload": {
                    "protocol": "forest.payload-json-from-prior/1",
                    "prior_step_id": "prepare",
                },
                "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
                "memory": {},
            },
            operation_id=ctx.operations.stable("commit-silent-input:v1"),
        )
        if result.get("status") != "BATCH" or result.get("kind") != "SILENT_DRAIN":
            raise ForestError(
                "CONVERSATION_SILENT_DRAIN_INVALID",
                "Silent drain did not commit an input batch",
                409,
            )
        return result

    @job("conversation.context-maintenance")
    async def context_maintenance(self, task, ctx):
        actor = task["payload"]["actor"]
        result = await ctx.uow.call(
            "conversation.compact-before-run",
            {
                "anchor": {
                    "protocol": "forest.conversation-pre-run-anchor/1",
                    "conversation_id": actor["conversation_id"],
                    "actor": actor,
                },
                "context_view": self._context_view,
                "payload": {
                    "protocol": "forest.payload-json-from-prior/1",
                    "prior_step_id": "prepare",
                },
                "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
                "memory": {},
            },
            operation_id=ctx.operations.stable("context-maintenance:v1"),
        )
        if result.get("conversation_id") != actor["conversation_id"]:
            raise ForestError(
                "CONVERSATION_MAINTENANCE_RESULT_INVALID",
                "Context maintenance returned another conversation",
                409,
            )
        return result

    @staticmethod
    def _stable(kind, *parts):
        encoded = "\0".join(str(part) for part in parts).encode("utf-8")
        return f"{kind}:{hashlib.sha256(encoded).hexdigest()}"

    @staticmethod
    def _defer(status, reason, delay):
        return {
            "protocol": "forest.conversation-agent-work/1",
            "disposition": "DEFER",
            "agent_status": status,
            "reason_code": reason,
            "delay_seconds": delay,
        }

    @staticmethod
    def _release(status):
        return {
            "protocol": "forest.conversation-agent-work/1",
            "disposition": "RELEASE",
            "agent_status": status,
            "reason_code": None,
            "delay_seconds": None,
        }

    async def _commit_final_reply(self, task, execution, ctx):
        reply = execution["reply"]
        if reply is None:
            raise ForestError(
                "CONVERSATION_AGENT_RESULT_INVALID",
                "Agent final boundary omitted its reply",
                409,
            )
        run_ref = execution["run_ref"]
        reply_ref = reply["owner_id"]
        intent_id = self._stable("delivery-intent", run_ref)
        part_id = self._stable("delivery-part", run_ref, reply_ref)
        await ctx.uow.call(
            "conversation.commit-final-reply",
            {
                "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
                "agent": {
                    "run_id": run_ref,
                    "session_id": execution["session_ref"],
                    "reply": reply,
                },
                "conversation": {
                    "actor": task["actor"],
                    "session_ref": execution["session_ref"],
                    "run_ref": run_ref,
                    "reply": reply,
                    "unit_id": self._stable("context-reply", run_ref),
                },
                "memory": {
                    "conversation_ref": task["conversation_id"],
                    "reply_ref": reply_ref,
                },
                "delivery": {
                    "protocol": "forest.delivery-intent/1",
                    "intent_id": intent_id,
                    "binding_ref": task["binding_ref"],
                    "lane_kind": "CHAT",
                    "ordering_key": task["conversation_id"],
                    "conversation_ref": task["conversation_id"],
                    "channel_kind": task["channel_kind"],
                    "channel_ref": task["channel_ref"],
                    "cause_component_id": "conversation.scheduler",
                    "cause_kind": "context.reply",
                    "cause_id": reply_ref,
                    "reply_ref": reply_ref,
                    "parts": [
                        {
                            "part_operation_id": part_id,
                            "effect_operation_ref": self._stable("delivery-effect", part_id),
                            "payload": reply,
                        }
                    ],
                },
            },
            operation_id=ctx.operations.stable(f"commit-final-reply:{run_ref}"),
            timeout=35,
        )

    async def _handoff_once(self, task, execution, deadline, ctx):
        completed = execution["run_ref"]
        successor = self._stable(
            "run-successor", task["conversation_id"], completed, task["actor"]["epoch"]
        )
        return await ctx.uow.call(
            "conversation.session-handoff",
            {
                "conversation": {
                    "actor": task["actor"],
                    "boundary": "SESSION_END",
                    "selection_mode": "ANY",
                    "session_ref": execution["session_ref"],
                    "run_ref": completed,
                    "next_run_ref": successor,
                },
                "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
                "memory": {
                    "selection_step_id": "conversation",
                    "deletion_epoch_step_id": "payload_epoch",
                },
                "agent": {
                    "session_id": execution["session_ref"],
                    "completed_run_id": completed,
                    "successor_run_id": successor,
                    "successor_deadline": deadline,
                    "selection_step_id": "conversation",
                    "memory_step_id": "memory",
                },
            },
            operation_id=ctx.operations.stable(f"session-handoff:{completed}"),
            timeout=35,
        )

    async def _handoff(self, task, execution, deadline, ctx):
        for _ in range(2):
            try:
                return await self._handoff_once(task, execution, deadline, ctx)
            except ForestError as exc:
                if exc.code != "CONVERSATION_PREFILL_REQUIRED":
                    raise
                for _ in range(8):
                    selected = await self.prefill_trigger_context(
                        {
                            "protocol": "forest.conversation-context-prefill/1",
                            "request_id": uuid4().hex,
                            "actor": task["actor"],
                            "boundary": "SESSION_END",
                            "session_ref": execution["session_ref"],
                            "run_ref": execution["run_ref"],
                        },
                        ctx,
                    )
                    if selected["status"] == "CONTINUE":
                        break
                else:
                    return {"action": "WAIT_INPUT"}
        return {"action": "WAIT_INPUT"}

    async def _commit_terminal(self, task, execution, ctx):
        reason_code = execution["error_code"]
        if (
            not isinstance(reason_code, str)
            or not reason_code
        ):
            raise ForestError(
                "CONVERSATION_AGENT_RESULT_INVALID",
                "Terminal Agent result omitted its reason",
                409,
            )
        await ctx.uow.call(
            "conversation.commit-terminal-run",
            {
                "conversation": {
                    "actor": task["actor"],
                    "session_ref": execution["session_ref"],
                    "run_ref": execution["run_ref"],
                    "terminal_status": execution["status"],
                    "reason_code": reason_code,
                }
            },
            operation_id=ctx.operations.stable(
                f"commit-terminal-run:{execution['run_ref']}"
            ),
            timeout=25,
        )

    @job("conversation.agent-run")
    async def agent_run(self, task, ctx):
        work = task["payload"]
        policy = await ctx.services.call(
            "agent.execution.policy",
            {"protocol": "forest.agent-execution-policy-query/1"},
            operation_id=ctx.operations.stable("agent-execution-policy"),
            timeout=10,
        )
        claimed_at = datetime.fromisoformat(work["claimed_at"].replace("Z", "+00:00"))
        if claimed_at.tzinfo is None:
            raise ForestError(
                "CONVERSATION_AGENT_CLAIM_INVALID",
                "Agent work claim time has no timezone",
                409,
            )
        new_run_deadline = (
            claimed_at.astimezone(UTC)
            + timedelta(seconds=policy["run_timeout_seconds"])
        ).isoformat()
        run_ref = work["run_ref"]
        resume = work["resume"]
        if not resume:
            await ctx.uow.call(
                "conversation.compact-before-run",
                {
                    "anchor": {
                        "protocol": "forest.conversation-pre-run-anchor/1",
                        "conversation_id": work["conversation_id"],
                        "actor": work["actor"],
                    },
                    "context_view": self._context_view,
                    "payload": {
                        "protocol": "forest.payload-json-from-prior/1",
                        "prior_step_id": "prepare",
                    },
                    "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
                    "memory": {},
                },
                operation_id=ctx.operations.stable(f"compact-before-run:{run_ref}"),
            )
        while True:
            execution = await ctx.services.call(
                "agent.execute",
                {
                    "protocol": "forest.agent-execute/2",
                    "conversation_id": work["conversation_id"],
                    "session_ref": work["session_ref"],
                    "run_ref": run_ref,
                    "resume": resume,
                    "absolute_deadline": None if resume else new_run_deadline,
                    "actor": work["actor"],
                },
                operation_id=ctx.operations.stable(
                    f"agent-execute:{run_ref}:{work['actor']['epoch']}"
                ),
                contract_major=2,
                timeout=540,
            )
            status = execution["status"]
            if status == "IDLE":
                return self._release("IDLE")
            if status == "FINAL_READY":
                await self._commit_final_reply(work, execution, ctx)
                return self._defer("COMPLETED", "session_handoff_pending", 0.1)
            if status == "COMPLETED":
                handoff = await self._handoff(work, execution, new_run_deadline, ctx)
                if handoff["action"] == "WAIT_INPUT":
                    return self._defer("WAIT_INPUT", "classification_prefix_pending", 0.25)
                if handoff["action"] == "CLOSED":
                    return self._release("COMPLETED")
                if handoff["action"] != "SUCCESSOR" or handoff["run"] is None:
                    raise ForestError(
                        "CONVERSATION_SESSION_HANDOFF_INVALID",
                        "Session handoff returned an invalid action",
                        409,
                    )
                run_ref = handoff["run"]["run_id"]
                resume = True
                continue
            if status == "WAIT_INPUT":
                return self._defer(status, "classification_prefix_pending", 0.25)
            if status == "WAIT_MEMORY":
                return self._defer(status, "memory_recall_pending", 2.0)
            if status == "WAIT_APPROVAL":
                return self._defer(status, "approval_pending", 1.0)
            if status == "RELOAD_WAIT":
                return self._defer(status, "agent_reload_pending", 0.5)
            if status in {"WAIT_TOOL", "BLOCKED_UNKNOWN"}:
                return self._defer(status, "agent_reconciliation_pending", 30.0)
            if status in {"FAILED", "CANCELLED"}:
                await self._commit_terminal(work, execution, ctx)
                return self._release(status)
            raise ForestError(
                "CONVERSATION_AGENT_RESULT_INVALID",
                "Agent execution returned an unknown state",
                409,
            )

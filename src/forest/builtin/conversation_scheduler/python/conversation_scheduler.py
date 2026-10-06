import hashlib
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from forest.errors import ForestError
from forest.sdk import Plugin, job, service


class ConversationScheduler(Plugin):
    MAX_PRE_RUN_COMPACTION_SEGMENTS = 8

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
            != {
                "high_watermark_units",
                "retain_units",
                "high_watermark_payload_bytes",
                "retain_payload_bytes",
                "high_watermark_media_bytes",
                "retain_media_bytes",
                "high_watermark_decoded_bytes",
                "retain_decoded_bytes",
                "policy_revision",
            }
            or type(view["high_watermark_units"]) is not int
            or type(view["retain_units"]) is not int
            or not 2 <= view["high_watermark_units"] <= 1_000_000
            or not 1 <= view["retain_units"] < view["high_watermark_units"]
            or type(view["high_watermark_payload_bytes"]) is not int
            or type(view["retain_payload_bytes"]) is not int
            or not 1024 <= view["high_watermark_payload_bytes"] <= 134_217_728
            or not 1 <= view["retain_payload_bytes"] < view["high_watermark_payload_bytes"]
            or type(view["high_watermark_media_bytes"]) is not int
            or type(view["retain_media_bytes"]) is not int
            or not 1024 <= view["high_watermark_media_bytes"] <= 134_217_728
            or not 1 <= view["retain_media_bytes"] < view["high_watermark_media_bytes"]
            or type(view["high_watermark_decoded_bytes"]) is not int
            or type(view["retain_decoded_bytes"]) is not int
            or not 1024 <= view["high_watermark_decoded_bytes"] <= 536_870_912
            or not 1 <= view["retain_decoded_bytes"] < view["high_watermark_decoded_bytes"]
            or view["policy_revision"] != "context-view-multimodal-window-v4"
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
        "conversation.media-cost.record",
        semantics="idempotent",
        contract="2.0",
        input_schema="schemas/media-cost-input.json",
        output_schema="schemas/media-cost-output.json",
    )
    async def record_media_cost(self, payload, ctx):
        return await ctx.commands.call(
            "conversation.media-cost.write", payload,
            operation_id=ctx.operations.stable(
                "media-cost:" + payload["event_id"] + ":" + payload["part_id"]
            ),
        )

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
            "next_run_ref": request["next_run_ref"],
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
            if result["kind"] not in {"CONTEXT_PREFILL", "RECOVERY_PREFILL"} or len(result["event_ids"]) > 64:
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
            for media in unit["media_parts"]:
                reference = media["payload"]
                parents[reference["payload_id"]] = reference
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
            "required_for_run": False,
            "media_parts": [],
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

    @service("conversation.context.snapshot", semantics="idempotent", contract="5.0")
    async def context_snapshot(self, payload, ctx):
        actor = payload["actor"]
        run_ref = payload.get("run_ref")
        offset = 0
        metadata = None
        payload_bytes = None
        raw_pages = []
        archive_present = False
        while True:
            page_request = {"actor": actor, "offset": offset, "limit": 32}
            if run_ref is not None:
                page_request["run_ref"] = run_ref
            page = await ctx.queries.call(
                "conversation.context-page",
                page_request,
                contract_major=4,
            )
            current_metadata = {
                key: page[key]
                for key in (
                    "conversation_id",
                    "run_ref",
                    "claimed_trigger_count",
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
                    "context_view_media_bytes",
                    "context_view_decoded_bytes",
                    "media_cost_revision",
                    "context_archived_unit_count",
                    "archive",
                    "total_units",
                )
            }
            if metadata is None:
                metadata = current_metadata
                payload_bytes = page["context_view_payload_bytes"]
                if metadata["total_units"] > 4096:
                    raise ForestError(
                        "CONTEXT_VIEW_HARD_LIMIT",
                        "Active Context exceeds the bounded snapshot size", 413,
                    )
            elif (current_metadata != metadata
                  or page["context_view_payload_bytes"] != payload_bytes):
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
                raw_pages.append((offset, units))
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
        required_trigger_unit_ids = [
            unit["unit_id"]
            for _, units in raw_pages
            for unit in units
            if unit["required_for_run"]
        ]
        if (
            len(required_trigger_unit_ids) != metadata["claimed_trigger_count"]
            or len(set(required_trigger_unit_ids)) != len(required_trigger_unit_ids)
            or (run_ref is None and required_trigger_unit_ids)
        ):
            raise ForestError(
                "CONTEXT_REQUIRED_TRIGGER_CONFLICT",
                "Claimed triggers differ from active Context units", 409,
            )
        if metadata["context_view_unit_count"] != metadata["total_units"]:
            raise ForestError(
                "CONTEXT_SNAPSHOT_COUNT_CONFLICT",
                "Conversation Context count differs from its materialized view",
                409,
            )
        materialized_payload_bytes = sum(
            unit["payload_byte_length"]
            for page_offset, units in raw_pages
            for index, unit in enumerate(units)
            if not (archive_present and page_offset == 0 and index == 0)
        )
        if materialized_payload_bytes != payload_bytes:
            raise ForestError(
                "CONTEXT_SNAPSHOT_PAYLOAD_COST_CONFLICT",
                "Conversation Payload facts differ from the active window", 409,
            )
        if payload_bytes > self._context_view["high_watermark_payload_bytes"]:
            raise ForestError(
                "CONTEXT_VIEW_HARD_LIMIT",
                "Required Context Payload exceeds the configured model-view bound", 413,
            )
        media_bytes = sum(
            part["byte_length"] for _, units in raw_pages
            for unit in units for part in unit["media_parts"]
        )
        if media_bytes != metadata["context_view_media_bytes"]:
            raise ForestError(
                "CONTEXT_SNAPSHOT_MEDIA_COST_CONFLICT",
                "Conversation media facts differ from the active window", 409,
            )
        if media_bytes > self._context_view["high_watermark_media_bytes"]:
            raise ForestError(
                "CONTEXT_VIEW_MEDIA_OVER_BUDGET",
                "Required Context media exceeds the configured model-view bound", 413,
            )
        decoded_bytes = sum(
            part["decoded_pixels"] * 4
            for _, units in raw_pages
            for unit in units
            for part in unit["media_parts"]
        )
        if decoded_bytes != metadata["context_view_decoded_bytes"]:
            raise ForestError(
                "CONTEXT_SNAPSHOT_DECODED_COST_CONFLICT",
                "Conversation decoded media facts differ from the active window", 409,
            )
        if decoded_bytes > self._context_view["high_watermark_decoded_bytes"]:
            raise ForestError(
                "CONTEXT_VIEW_DECODED_OVER_BUDGET",
                "Required Context decoded media exceeds the model-view bound", 413,
            )
        pages = []
        for offset, units in raw_pages:
            saved = await ctx.payloads.put_json(
                {
                    "protocol": "forest.conversation-context-page/1",
                    "conversation_id": metadata["conversation_id"],
                    "context_revision": metadata["context_revision"],
                    "context_view_revision": metadata["context_view_revision"],
                    "offset": offset,
                    "units": units,
                },
                owner_kind="context.snapshot.page",
                owner_id=ctx.operations.stable(f"context-snapshot-page:{offset}"),
                parents=self._unit_parents(units),
            )
            pages.append(self._exact(saved))
        roots, tree_depth = await self._snapshot_tree(pages, ctx)
        owner_id = ctx.operations.stable("context-snapshot-manifest:v5")
        document = {
            "protocol": "forest.conversation-context-snapshot/5",
            **metadata,
            "required_trigger_unit_ids": required_trigger_unit_ids,
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
            "protocol": "forest.conversation-context-snapshot/5",
            "conversation_id": metadata["conversation_id"],
            "run_ref": metadata["run_ref"],
            "claimed_trigger_count": metadata["claimed_trigger_count"],
            "context_applied_seq": metadata["context_applied_seq"],
            "context_revision": metadata["context_revision"],
            "context_view_revision": metadata["context_view_revision"],
            "media_cost_revision": metadata["media_cost_revision"],
            "context_view_media_bytes": media_bytes,
            "context_view_decoded_bytes": decoded_bytes,
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

    @job("conversation.trigger-settlement")
    async def trigger_settlement(self, task, ctx):
        claim = task["payload"]
        if claim["lease_epoch"] != task["attempt"]:
            raise ForestError(
                "TRIGGER_SETTLEMENT_CLAIM_INVALID",
                "Trigger settlement lease epoch differs from its task",
                409,
            )
        result = await ctx.uow.call(
            "conversation.settle-trigger-segment",
            claim,
            operation_id=ctx.operations.stable(
                "settle-trigger-segment:"
                f"{claim['claim_ref']}:{claim['segment_index']}:{claim['claim_token']}"
            ),
            timeout=20,
        )
        if (
            result["claim_ref"] != claim["claim_ref"]
            or result["segment_index"] != claim["segment_index"]
        ):
            raise ForestError(
                "TRIGGER_SETTLEMENT_RESULT_INVALID",
                "Settled trigger segment differs from its claim",
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
        reply_byte_length = len(await ctx.payloads.read_bytes(reply))
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
                    "reply_byte_length": reply_byte_length,
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
        successor = self._stable("run-successor", task["conversation_id"], completed)
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
        successor = self._stable(
            "run-successor", task["conversation_id"], execution["run_ref"]
        )
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
                            "next_run_ref": successor,
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

    async def _requeue_pending(self, task, session_ref, run_ref, ctx, *, allow_absent_run):
        request = await ctx.queries.call(
            "conversation.read-requeue-request",
            {"conversation_id": task["conversation_id"],
             "run_ref": run_ref},
            timeout=10,
        )
        if request["state"] != "PENDING":
            return False
        if (request["session_ref"] != session_ref
                or request["conversation_id"] != task["conversation_id"]):
            raise ForestError(
                "CONVERSATION_REQUEUE_REQUEST_INVALID",
                "Pending requeue request differs from the terminal Run",
                409,
            )
        try:
            await ctx.uow.call(
                "conversation.requeue-run",
                {
                    "conversation": {
                        "protocol": "forest.conversation-run-requeue/1",
                        "actor": task["actor"],
                        "request_id": request["request_id"],
                        "session_ref": request["session_ref"],
                        "run_ref": run_ref,
                        "claim_ref": request["claim_ref"],
                        "claim_digest": request["claim_digest"],
                        "reason_code": request["reason_code"],
                    },
                    "agent": {"protocol": "forest.agent-requeue-verification/1",
                              "prior_step_id": "conversation",
                              "allow_absent_run": allow_absent_run},
                },
                operation_id=ctx.operations.stable(
                    f"requeue:{request['request_id']}:{task['actor']['epoch']}"
                ),
                timeout=25,
            )
        except ForestError as exc:
            if exc.code != "AGENT_REQUEUE_EFFECT_UNSAFE" and not (
                allow_absent_run and exc.code == "AGENT_RUN_NOT_ABANDONED"
            ):
                raise
            return False
        return True

    async def _compact_before_run(self, work, ctx, *, completed=None):
        actor = work["actor"]
        started_at = time.monotonic()
        if completed is None:
            anchor = {
                "protocol": "forest.conversation-pre-run-anchor/1",
                "conversation_id": work["conversation_id"],
                "actor": actor,
            }
            plan_name = "conversation.compact-before-run"
            operation_kind = "compact-before-run"
        else:
            anchor = {
                "protocol": "forest.conversation-handoff-compaction-anchor/1",
                "conversation_id": work["conversation_id"],
                "actor": actor,
                "session_ref": completed["session_ref"],
                "run_ref": completed["run_ref"],
            }
            plan_name = "conversation.compact-before-handoff"
            operation_kind = "compact-before-handoff"
        for segment_index in range(self.MAX_PRE_RUN_COMPACTION_SEGMENTS):
            # Keep room for the 540 s Agent service within this 600 s handler.
            if segment_index and time.monotonic() - started_at >= 20:
                return False
            result = await ctx.uow.call(
                plan_name,
                {
                    "anchor": anchor,
                    "context_view": self._context_view,
                    "payload": {
                        "protocol": "forest.payload-json-from-prior/1",
                        "prior_step_id": "prepare",
                    },
                    "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
                    "memory": {},
                },
                operation_id=ctx.operations.stable(
                    f"{operation_kind}:{anchor.get('run_ref',work['run_ref'])}:"
                    f"{actor['epoch']}:{segment_index}"
                ),
            )
            if (result.get("protocol") != "forest.context-pre-run-compaction/1"
                    or result.get("conversation_id") != work["conversation_id"]):
                raise ForestError(
                    "CONVERSATION_PRE_RUN_COMPACTION_INVALID",
                    "Pre-run Context compaction returned another conversation", 409,
                )
            if result["status"] == "SKIPPED":
                return True
            if result["status"] != "COMPACTED":
                raise ForestError(
                    "CONVERSATION_PRE_RUN_COMPACTION_INVALID",
                    "Pre-run Context compaction returned an unknown state", 409,
                )
        return False

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
        if await self._requeue_pending(
            work, work["session_ref"], run_ref, ctx, allow_absent_run=True
        ):
            return self._release("REQUEUED")
        if not resume and not await self._compact_before_run(work, ctx):
            return self._defer("WAIT_CONTEXT", "CONTEXT_PRE_RUN_MORE", 0.5)
        execute_revision = 0
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
                    f"agent-execute:{run_ref}:{work['actor']['epoch']}:{execute_revision}"
                ),
                contract_major=2,
                timeout=540,
            )
            status = execution["status"]
            if status == "IDLE":
                return self._release("IDLE")
            if status == "FINAL_READY":
                try:
                    await self._commit_final_reply(work, execution, ctx)
                except ForestError as exc:
                    if exc.code != "AGENT_CANCEL_REQUESTED" or execute_revision >= 2:
                        raise
                    execute_revision += 1
                    continue
                return self._defer("COMPLETED", "session_handoff_pending", 0.1)
            if status == "COMPLETED":
                if not await self._compact_before_run(
                    work, ctx, completed=execution
                ):
                    return self._defer("WAIT_CONTEXT", "CONTEXT_HANDOFF_MORE", 0.5)
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
                execute_revision = 0
                continue
            if status == "WAIT_INPUT":
                return self._defer(status, "classification_prefix_pending", 0.25)
            if status == "WAIT_OWNER":
                return self._defer(status, "agent_owner_lease_pending", 30.0)
            if status == "WAIT_MEMORY":
                return self._defer(status, "memory_recall_pending", 2.0)
            if status == "WAIT_APPROVAL":
                return self._defer(status, "approval_pending", 1.0)
            if status == "RELOAD_WAIT":
                return self._defer(status, "agent_reload_pending", 0.5)
            if status in {"WAIT_TOOL", "BLOCKED_UNKNOWN"}:
                return self._defer(status, "agent_reconciliation_pending", 30.0)
            if status in {"FAILED", "CANCELLED"}:
                if await self._requeue_pending(
                    work, execution["session_ref"], execution["run_ref"], ctx,
                    allow_absent_run=False,
                ):
                    return self._release("REQUEUED")
                await self._commit_terminal(work, execution, ctx)
                return self._release(status)
            raise ForestError(
                "CONVERSATION_AGENT_RESULT_INVALID",
                "Agent execution returned an unknown state",
                409,
            )

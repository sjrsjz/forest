from collections.abc import Mapping

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

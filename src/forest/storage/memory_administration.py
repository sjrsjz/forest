"""Reviewed source listing and public memory-forget plan/apply operations."""

from __future__ import annotations

import base64
import binascii
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.control import BotControl
from forest.storage.memory_sources import MemorySourceStore
from forest.storage.payloads import PayloadStore


def _clone(value):
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise ForestError("MEMORY_MANAGEMENT_INVALID", "Memory request must be JSON", 422) from None


def _identifier(value, *, maximum=256):
    if (not isinstance(value, str) or not 1 <= len(value) <= maximum
            or any(ord(char) < 32 for char in value)):
        raise ForestError("MEMORY_MANAGEMENT_INVALID", "Memory identifier is invalid", 422)
    return value


def _instant(value, name):
    if value is None or isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
    else:
        parsed = None
    if parsed is None or parsed.tzinfo is None:
        raise ForestError("MEMORY_MANAGEMENT_INVALID", f"{name} must include a timezone", 422)
    return parsed.astimezone(timezone.utc)


def _source_cursor(filters, position):
    occurred_at, source_unit_id = position
    value = {
        "v": 1,
        "filters": digest_json(filters),
        "occurred_at": occurred_at.isoformat(),
        "source_unit_id": source_unit_id,
    }
    raw = json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _read_source_cursor(value, filters):
    if value is None:
        return None, None
    _identifier(value, maximum=512)
    try:
        padded = value + "=" * (-len(value) % 4)
        decoded = json.loads(base64.urlsafe_b64decode(padded).decode())
        if (
            set(decoded) != {"v", "filters", "occurred_at", "source_unit_id"}
            or decoded["v"] != 1
            or decoded["filters"] != digest_json(filters)
        ):
            raise ValueError
        source_unit_id = _identifier(decoded["source_unit_id"])
        occurred_at = _instant(decoded["occurred_at"], "Source cursor timestamp")
    except (ValueError, TypeError, KeyError, UnicodeError, json.JSONDecodeError, binascii.Error):
        raise ForestError(
            "MEMORY_SOURCE_CURSOR_INVALID",
            "Source cursor is invalid or belongs to different filters",
            422,
        ) from None
    return occurred_at, source_unit_id


class MemoryAdministration:
    def __init__(self, messages, *, control=None):
        self.messages, self.store = messages, messages.store
        self.control = control or BotControl(self.store)
        self.sources = MemorySourceStore(messages)
        self.payloads = PayloadStore(self.store)

    async def _group_scope_in(self, conn, claims, requested=None):
        conversation_id = claims.get("source_conversation_id")
        if conversation_id is None:
            return requested
        row = await (await conn.execute(
            "SELECT group_source_id FROM public.bot_conversations "
            "WHERE bot_id=%s AND conversation_id=%s",
            (self.store.bot_id, conversation_id),
        )).fetchone()
        if row is None or (requested is not None and requested != row["group_source_id"]):
            raise ForestError(
                "SHELL_SCOPE_DENIED", "Group commands can manage only their own memory trunk", 403
            )
        return row["group_source_id"]

    async def _require_plan_group_in(self, conn, plan, group_source_id):
        if group_source_id is None:
            return
        identities = [item["source_unit_id"] for item in plan["requested_targets"]]
        rows = await (await conn.execute(
            "SELECT u.source_unit_id,s.target_group_source_id "
            "FROM public.bot_memory_source_units u JOIN public.bot_memory_sources s "
            "USING(bot_id,source_id) WHERE u.bot_id=%s AND u.source_unit_id=ANY(%s)",
            (self.store.bot_id, identities),
        )).fetchall()
        if (len(rows) != len(identities)
                or any(row["target_group_source_id"] != group_source_id for row in rows)):
            raise ForestError(
                "SHELL_SCOPE_DENIED", "Group commands can manage only their own memory trunk", 403
            )

    @staticmethod
    def _public_source(row, *, include_parts=True):
        source = {
            key: row[key]
            for key in (
                "source_unit_id", "source_revision", "source_order_seq", "kind", "occurred_at",
                "accepted_at", "source", "author",
            )
            if key in row
        }
        if hasattr(source.get("occurred_at"), "isoformat"):
            source["occurred_at"] = source["occurred_at"].isoformat()
        if hasattr(source.get("accepted_at"), "isoformat"):
            source["accepted_at"] = source["accepted_at"].isoformat()
        if include_parts:
            source["parts"] = row.get("parts", [])
        return source

    async def read(self, ticket, operation_id, payload=None):
        payload = _clone({} if payload is None else payload)
        allowed = {
            "after", "limit", "group_source_id", "principal_ref", "kind", "has_image",
            "occurred_from", "occurred_before",
        }
        if set(payload) - allowed:
            raise ForestError("MEMORY_MANAGEMENT_INVALID", "Unknown memory source selector", 422)
        after = payload.get("after")
        limit = payload.get("limit", 50)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ForestError("MEMORY_MANAGEMENT_INVALID", "Source page limit is 1 to 100", 422)
        group = payload.get("group_source_id")
        if group is not None:
            _identifier(group)
        principal = payload.get("principal_ref")
        if principal is not None:
            _identifier(principal)
        kind = payload.get("kind")
        if kind not in {None, "INPUT", "REPLY"}:
            raise ForestError("MEMORY_MANAGEMENT_INVALID", "Source kind is invalid", 422)
        has_image = payload.get("has_image")
        if has_image is not None and type(has_image) is not bool:
            raise ForestError("MEMORY_MANAGEMENT_INVALID", "Image filter is invalid", 422)
        occurred_from = _instant(payload["occurred_from"], "Start time") if payload.get("occurred_from") else None
        occurred_before = _instant(payload["occurred_before"], "End time") if payload.get("occurred_before") else None
        if occurred_from and occurred_before and occurred_from >= occurred_before:
            raise ForestError("MEMORY_MANAGEMENT_INVALID", "Start time must precede end time", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "memory.forget.read", operation_id, payload
            )
            group = await self._group_scope_in(conn, claims, group)
            filters = {
                "group_source_id": group,
                "principal_ref": principal,
                "kind": kind,
                "has_image": has_image,
                "occurred_from": occurred_from.isoformat() if occurred_from else None,
                "occurred_before": occurred_before.isoformat() if occurred_before else None,
            }
            before_time, before_id = _read_source_cursor(after, filters)
            view = await self.sources.authorize_sources_in(
                conn, group_source_id=group, browse=True,
                before_occurred_at=before_time, before_source_unit_id=before_id,
                principal_ref=principal, source_kind=kind, has_image=has_image,
                occurred_from=occurred_from, occurred_before=occurred_before,
                limit=limit, include_payload=True,
            )
            recent = await (await conn.execute(
                "SELECT operation_id,deletion_epoch,jsonb_array_length(targets) AS target_count,created_at "
                "FROM public.bot_payload_forget_requests WHERE bot_id=%s "
                "ORDER BY deletion_epoch DESC LIMIT 20",
                (self.store.bot_id,),
            )).fetchall()
            revoked = await (await conn.execute(
                "SELECT count(*) AS count FROM public.bot_payload_headers "
                "WHERE bot_id=%s AND state='REVOKED'", (self.store.bot_id,),
            )).fetchone()
            links = await (await conn.execute(
                "SELECT count(*) AS count FROM public.bot_payload_source_links l "
                "JOIN public.bot_payload_headers p ON p.bot_id=l.bot_id AND p.payload_id=l.parent_payload_id "
                "WHERE l.bot_id=%s AND p.state IN ('REVOKED','SCRUBBED') AND NOT EXISTS("
                "SELECT 1 FROM public.bot_payload_suppression x WHERE x.bot_id=l.bot_id "
                "AND x.source_unit_id=l.dependent_source_unit_id AND x.part_id='*')",
                (self.store.bot_id,),
            )).fetchone()
            await self.control._time(conn, claims)
            sources = [self._public_source(row) for row in view["sources"]]
            return {
                "deletion_epoch": view["deletion_epoch"],
                "fact_revision": view["fact_revision"],
                "group_source_id": group,
                "filters": filters,
                "sources": sources,
                "next_after": (
                    _source_cursor(filters, view["next_position"])
                    if view["next_position"] else None
                ),
                "scrub": {"revoked_payloads": revoked["count"], "pending_links": links["count"]},
                "recent": [{**row, "created_at": row["created_at"].isoformat()} for row in recent],
            }

    async def trunks(self, ticket, operation_id, payload=None):
        payload = _clone({} if payload is None else payload)
        if set(payload) - {"after", "limit", "platform", "channel_kind", "channel_id"}:
            raise ForestError("MEMORY_MANAGEMENT_INVALID", "Unknown memory trunk selector", 422)
        after = payload.get("after", "")
        if after:
            _identifier(after)
        limit = payload.get("limit", 50)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ForestError("MEMORY_MANAGEMENT_INVALID", "Trunk page limit is 1 to 100", 422)
        selectors = {}
        for key in ("platform", "channel_kind", "channel_id"):
            value = payload.get(key)
            if value is not None:
                selectors[key] = _identifier(value)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "memory.forget.read", operation_id, payload
            )
            group = await self._group_scope_in(conn, claims)
            rows = await (await conn.execute(
                "SELECT s.target_group_source_id AS group_source_id,min(s.platform) AS platform,"
                "min(s.realm_id) AS realm_id,min(s.channel_kind) AS channel_kind,"
                "min(s.channel_id) AS channel_id,count(u.source_unit_id) AS registered_sources,"
                "count(u.source_unit_id) FILTER (WHERE h.state='ACTIVE' AND EXISTS("
                "SELECT 1 FROM public.bot_payload_bodies b WHERE b.bot_id=h.bot_id "
                "AND b.payload_id=h.payload_id)) AS stored_sources,"
                "count(DISTINCT u.principal_ref) AS authors,"
                "count(u.source_unit_id) FILTER (WHERE u.kind='INPUT') AS inputs,"
                "count(u.source_unit_id) FILTER (WHERE u.kind='REPLY') AS replies,"
                "count(u.source_unit_id) FILTER (WHERE EXISTS(SELECT 1 "
                "FROM public.bot_memory_source_media m WHERE m.bot_id=u.bot_id "
                "AND m.source_unit_id=u.source_unit_id)) AS image_sources,"
                "max(u.occurred_at) AS latest_occurred_at "
                "FROM public.bot_memory_sources s "
                "LEFT JOIN public.bot_memory_source_units u USING(bot_id,source_id) "
                "LEFT JOIN public.bot_payload_headers h ON h.bot_id=u.bot_id AND h.payload_id=u.payload_id "
                "WHERE s.bot_id=%s AND s.source_kind='GROUP' "
                "AND s.target_group_source_id>%s "
                "AND (%s::text IS NULL OR s.target_group_source_id=%s) "
                "AND (%s::text IS NULL OR s.platform=%s) "
                "AND (%s::text IS NULL OR s.channel_kind=%s) "
                "AND (%s::text IS NULL OR s.channel_id=%s) "
                "GROUP BY s.target_group_source_id ORDER BY s.target_group_source_id LIMIT %s",
                (
                    self.store.bot_id, after, group, group,
                    selectors.get("platform"), selectors.get("platform"),
                    selectors.get("channel_kind"), selectors.get("channel_kind"),
                    selectors.get("channel_id"), selectors.get("channel_id"), limit,
                ),
            )).fetchall()
            await self.control._time(conn, claims)
            trunks = []
            for row in rows:
                item = dict(row)
                if item["latest_occurred_at"] is not None:
                    item["latest_occurred_at"] = item["latest_occurred_at"].isoformat()
                trunks.append(item)
            return {
                "trunks": trunks,
                "next_after": rows[-1]["group_source_id"] if len(rows) == limit else None,
            }

    async def plan(self, ticket, operation_id, payload):
        payload = _clone(payload)
        if set(payload) != {"expected_deletion_epoch", "source_unit_ids"}:
            raise ForestError("MEMORY_MANAGEMENT_INVALID", "Forget plan fields differ", 422)
        epoch, identities = payload["expected_deletion_epoch"], payload["source_unit_ids"]
        if type(epoch) is not int or epoch < 0 or not isinstance(identities, list):
            raise ForestError("MEMORY_MANAGEMENT_INVALID", "Forget plan is invalid", 422)
        if not 1 <= len(identities) <= 32 or len(set(identities)) != len(identities):
            raise ForestError("MEMORY_MANAGEMENT_INVALID", "Select 1 to 32 distinct sources", 422)
        for identity in identities:
            _identifier(identity)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "memory.forget.plan", operation_id, payload
            )
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                return replay
            group = await self._group_scope_in(conn, claims)
            view = await self.sources.authorize_sources_in(
                conn, group_source_id=group, source_unit_ids=identities,
                limit=len(identities), include_payload=True,
            )
            selected = {row["source_unit_id"]: row for row in view["sources"]}
            if set(selected) != set(identities):
                raise ForestError(
                    "MEMORY_SOURCE_NOT_FOUND", "A selected source is unavailable or outside scope", 404
                )
            refs = tuple(SourceRef(identity, selected[identity]["source_revision"]) for identity in identities)
            preview = await self.payloads.preview_forget_in(
                conn, refs, expected_deletion_epoch=epoch
            )
            requested = [
                {"source_unit_id": ref.source_unit_id, "source_revision": ref.source_revision,
                 "part_id": ref.part_id}
                for ref in refs
            ]
            plan_id = str(uuid4())
            plan_digest = digest_json({
                "bot_id": self.store.bot_id, "subject_id": claims["subject_id"],
                "issuer_id": claims["issuer_id"], "expected_deletion_epoch": epoch,
                "requested_targets": requested,
                "resolved_targets": preview["resolved_targets"],
            })
            expires = await self.store._now(conn) + timedelta(minutes=10)
            # The preview result contains source excerpts and is therefore one
            # additional source-derived payload created by this transaction.
            affected_payloads = preview["affected_payloads"] + 1
            await conn.execute(
                "INSERT INTO public.bot_memory_forget_plans(bot_id,plan_id,operation_id,subject_id,issuer_id,"
                "expected_deletion_epoch,requested_targets,resolved_targets,target_digest,affected_payloads,"
                "legacy_conservative,plan_digest,expires_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (self.store.bot_id, plan_id, operation_id, claims["subject_id"], claims["issuer_id"],
                 epoch, Jsonb(requested), Jsonb(preview["resolved_targets"]),
                 preview["input_digest"], affected_payloads,
                 preview["legacy_conservative"], plan_digest, expires),
            )
            result = {
                "plan_id": plan_id, "plan_digest": plan_digest,
                "expected_deletion_epoch": epoch,
                "source_count": len(requested),
                "resolved_target_count": len(preview["resolved_targets"]),
                "affected_payloads": affected_payloads,
                "legacy_conservative": preview["legacy_conservative"],
                "expires_at": expires.isoformat(),
                "sources": [self._public_source(selected[identity]) for identity in identities],
            }
            await self.control._record(
                conn, claims, result,
                parents=tuple(selected[identity]["payload_id"] for identity in identities),
            )
            return result

    async def apply(self, ticket, operation_id, payload, *, lease):
        payload = _clone(payload)
        if set(payload) != {"expected_deletion_epoch", "plan_id", "plan_digest"}:
            raise ForestError("MEMORY_MANAGEMENT_INVALID", "Forget apply fields differ", 422)
        epoch = payload["expected_deletion_epoch"]
        if type(epoch) is not int or epoch < 0:
            raise ForestError("MEMORY_MANAGEMENT_INVALID", "Deletion epoch is invalid", 422)
        _identifier(payload["plan_id"], maximum=128)
        _identifier(payload["plan_digest"], maximum=128)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "memory.forget.apply", operation_id, payload,
                payload["plan_digest"],
            )
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                return replay
            plan = await (await conn.execute(
                "SELECT * FROM public.bot_memory_forget_plans "
                "WHERE bot_id=%s AND plan_id=%s FOR SHARE",
                (self.store.bot_id, payload["plan_id"]),
            )).fetchone()
            if plan is None or any(plan[key] != claims[key] for key in ("subject_id", "issuer_id")):
                raise ForestError("PLAN_NOT_FOUND", "Forget plan is unavailable", 404)
            if (plan["plan_digest"] != payload["plan_digest"]
                    or plan["expected_deletion_epoch"] != epoch):
                raise ForestError("PLAN_CONFLICT", "Forget plan does not match", 409)
            if plan["expires_at"] <= await self.store._now(conn):
                raise ForestError("PLAN_EXPIRED", "Preview the forget operation again", 409)
            group = await self._group_scope_in(conn, claims)
            await self._require_plan_group_in(conn, plan, group)
            refs = tuple(
                SourceRef(item["source_unit_id"], item["source_revision"], item["part_id"])
                for item in plan["requested_targets"]
            )
            forgotten = await self.payloads.forget_sources_in(
                conn, lease, operation_id, refs, expected_deletion_epoch=epoch,
                expected_resolved_targets=plan["resolved_targets"],
                expected_affected_payloads=plan["affected_payloads"],
            )
            result = {
                **forgotten, "plan_id": plan["plan_id"],
                "source_count": len(plan["requested_targets"]),
                "target_count": len(plan["resolved_targets"]),
                "affected_payloads": plan["affected_payloads"],
                "scrub_pending": True,
            }
            await self.control._record(conn, claims, result)
            return result

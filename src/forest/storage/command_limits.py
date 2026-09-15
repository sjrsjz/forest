"""Versioned Bot-wide business-command admission limits."""

from __future__ import annotations

import json
from decimal import Decimal
from uuid import uuid4

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.config_payloads import ConfigPayloadStore
from forest.storage.control import BotControl

DEFAULT_COMMAND_LIMITS = {
    "max_argument_bytes": 16384,
    "max_pending_bot": 2048,
    "max_pending_binding": 512,
    "max_pending_principal": 8,
    "principal_burst": 8,
    "principal_refill_seconds": 2,
}

_BOUNDS = {
    "max_argument_bytes": (1, 1048576),
    "max_pending_bot": (1, 100000),
    "max_pending_binding": (1, 10000),
    "max_pending_principal": (1, 1000),
    "principal_burst": (1, 1000),
    "principal_refill_seconds": (1, 86400),
}


def normalize_command_limits(values) -> dict:
    try:
        copied = json.loads(json.dumps(values, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError, UnicodeError):
        raise ForestError("COMMAND_LIMIT_INVALID", "Command limits must be finite JSON", 422) from None
    if not isinstance(copied, dict) or set(copied) != set(_BOUNDS):
        raise ForestError("COMMAND_LIMIT_INVALID", "Command limit fields differ", 422)
    for key, (minimum, maximum) in _BOUNDS.items():
        if type(copied[key]) is not int or not minimum <= copied[key] <= maximum:
            raise ForestError("COMMAND_LIMIT_INVALID", f"{key} is outside its supported range", 422)
    if not (
        copied["max_pending_principal"]
        <= copied["max_pending_binding"]
        <= copied["max_pending_bot"]
    ):
        raise ForestError(
            "COMMAND_LIMIT_INVALID",
            "Principal, Binding and Bot pending limits must be ordered",
            422,
        )
    return copied


class CommandLimitStore:
    def __init__(self, store):
        self.store = store
        self.control = BotControl(store)
        self.content = ConfigPayloadStore(store)

    async def _head_in(self, conn):
        await conn.execute(
            "INSERT INTO public.bot_command_limit_heads(bot_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (self.store.bot_id,),
        )
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_command_limit_heads WHERE bot_id=%s FOR UPDATE",
                (self.store.bot_id,),
            )
        ).fetchone()

    async def _hydrate_in(self, conn, row, *, kind, required=True):
        if row is None:
            return None
        epoch = (await self.content.payloads.lock_in(conn))["deletion_epoch"]
        body = await self.content.hydrate_values_in(
            conn,
            row,
            family="command.limit",
            kind=kind,
            resource="",
            identity=row["plan_id"] if kind == "PLAN" else str(row["revision"]),
            epoch=epoch,
            required=required,
        )
        if body["values_payload_state"] == "ACTIVE":
            values = normalize_command_limits(body["values"])
            if any(row[key] != values[key] for key in _BOUNDS):
                raise ForestError("COMMAND_LIMIT_METADATA_CONFLICT", "Command limit metadata differs", 409)
            body = {**body, "values": values}
        return {**row, **body}

    async def _revision_in(self, conn, revision, *, required=True):
        if not revision:
            return None
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_command_limit_revisions WHERE bot_id=%s AND revision=%s",
                (self.store.bot_id, revision),
            )
        ).fetchone()
        return await self._hydrate_in(conn, row, kind="REVISION", required=required)

    async def read(self, ticket, operation_id: str) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "command.limit.read", operation_id, {})
            head = await self._head_in(conn)
            saved = await self._revision_in(conn, head["revision"], required=False)
            values = (
                saved["values"]
                if saved and saved["values_payload_state"] == "ACTIVE"
                else dict(DEFAULT_COMMAND_LIMITS) if saved is None else None
            )
            await self.control._time(conn, claims)
            return {
                "revision": head["revision"],
                "values": values,
                "defaults": dict(DEFAULT_COMMAND_LIMITS),
                "values_payload_state": saved["values_payload_state"] if saved else "DEFAULT",
            }

    async def plan(self, ticket, operation_id: str, payload: dict) -> dict:
        if (
            not isinstance(payload, dict)
            or set(payload) != {"expected_revision", "values"}
            or type(payload["expected_revision"]) is not int
            or payload["expected_revision"] < 0
        ):
            raise ForestError("COMMAND_LIMIT_INVALID", "Command limit plan fields differ", 422)
        values = normalize_command_limits(payload["values"])
        payload = {**payload, "values": values}
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "command.limit.plan", operation_id, payload
            )
            head = await self._head_in(conn)
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            if head["revision"] != payload["expected_revision"]:
                raise ForestError("REVISION_CONFLICT", "Command limits changed; reload them", 409)
            current = await self._revision_in(conn, head["revision"], required=False)
            plan_id = str(uuid4())
            plan_digest = digest_json(
                {
                    **payload,
                    "bot_id": self.store.bot_id,
                    "subject_id": claims["subject_id"],
                    "issuer_id": claims["issuer_id"],
                }
            )
            original = await self.control.admitted_input_in(conn, claims)
            epoch = (await self.content.payloads.lock_in(conn))["deletion_epoch"]
            ref = await self.content.put_values_in(
                conn,
                family="command.limit",
                kind="PLAN",
                resource="",
                identity=plan_id,
                values=values,
                parents=(original.payload_id,),
                epoch=epoch,
            )
            columns = ",".join(_BOUNDS)
            placeholders = ",".join(["%s"] * len(_BOUNDS))
            await conn.execute(
                "INSERT INTO public.bot_command_limit_plans("
                "bot_id,plan_id,subject_id,issuer_id,expected_revision,"
                + columns
                + ",plan_digest,expires_at,values_payload_id,values_payload_digest) VALUES ("
                + ",".join(["%s"] * 5)
                + ","
                + placeholders
                + ",%s,clock_timestamp()+interval '10 minutes',%s,%s)",
                (
                    self.store.bot_id,
                    plan_id,
                    claims["subject_id"],
                    claims["issuer_id"],
                    head["revision"],
                    *(values[key] for key in _BOUNDS),
                    plan_digest,
                    ref.payload_id,
                    ref.digest,
                ),
            )
            before = (
                current["values"]
                if current and current["values_payload_state"] == "ACTIVE"
                else dict(DEFAULT_COMMAND_LIMITS) if current is None else None
            )
            result = {
                "plan_id": plan_id,
                "plan_digest": plan_digest,
                "expected_revision": head["revision"],
                "values": values,
                "before_values": before,
                "before_values_payload_state": (
                    current["values_payload_state"] if current else "DEFAULT"
                ),
            }
            parents = (ref.payload_id,)
            if current and current["values_payload_state"] == "ACTIVE":
                parents += (current["values_payload_id"],)
            await self.control._record(conn, claims, result, parents=parents)
            await self.control._time(conn, claims)
            return result

    async def apply(self, ticket, operation_id: str, payload: dict) -> dict:
        if (
            not isinstance(payload, dict)
            or set(payload) != {"expected_revision", "plan_id", "plan_digest"}
            or type(payload["expected_revision"]) is not int
            or payload["expected_revision"] < 0
        ):
            raise ForestError("COMMAND_LIMIT_INVALID", "Command limit apply fields differ", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn,
                ticket,
                "command.limit.apply",
                operation_id,
                payload,
                payload["plan_digest"],
            )
            head = await self._head_in(conn)
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            plan = await (
                await conn.execute(
                    "SELECT * FROM public.bot_command_limit_plans WHERE bot_id=%s AND plan_id=%s",
                    (self.store.bot_id, payload["plan_id"]),
                )
            ).fetchone()
            if plan is None or any(plan[key] != claims[key] for key in ("subject_id", "issuer_id")):
                raise ForestError("PLAN_NOT_FOUND", "Command limit plan is unavailable", 404)
            if any(plan[key] != payload[key] for key in ("expected_revision", "plan_digest")):
                raise ForestError("PLAN_CONFLICT", "Command limit plan does not match", 409)
            if head["revision"] != payload["expected_revision"]:
                raise ForestError("REVISION_CONFLICT", "Command limits changed after preview", 409)
            if plan["expires_at"] <= await self.store._now(conn):
                raise ForestError("PLAN_EXPIRED", "Preview the command limits again", 409)
            plan = await self._hydrate_in(conn, plan, kind="PLAN")
            parent = await self.control.plan_parent(conn, "command.limit", plan)
            original = await self.control.admitted_input_in(conn, claims)
            revision = head["revision"] + 1
            epoch = (await self.content.payloads.lock_in(conn))["deletion_epoch"]
            ref = await self.content.put_values_in(
                conn,
                family="command.limit",
                kind="REVISION",
                resource="",
                identity=str(revision),
                values=plan["values"],
                parents=(original.payload_id, parent),
                epoch=epoch,
            )
            result = {"revision": revision, "status": "APPLIED"}
            await self.control._record(conn, claims, result, parents=(ref.payload_id,))
            columns = ",".join(_BOUNDS)
            placeholders = ",".join(["%s"] * len(_BOUNDS))
            await conn.execute(
                "INSERT INTO public.bot_command_limit_revisions("
                "bot_id,revision,previous_revision,"
                + columns
                + ",policy_revision,plan_id,operation_id,values_payload_id,values_payload_digest) VALUES ("
                + ",".join(["%s"] * 3)
                + ","
                + placeholders
                + ",%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    revision,
                    head["revision"],
                    *(plan["values"][key] for key in _BOUNDS),
                    claims["policy_revision"],
                    plan["plan_id"],
                    operation_id,
                    ref.payload_id,
                    ref.digest,
                ),
            )
            await conn.execute(
                "UPDATE public.bot_command_limit_heads SET revision=%s,updated_at=clock_timestamp() "
                "WHERE bot_id=%s",
                (revision, self.store.bot_id),
            )
            await self.control._time(conn, claims)
            if plan["expires_at"] <= await self.store._now(conn):
                raise ForestError("PLAN_EXPIRED", "Command limit plan expired before publication", 409)
            return result

    async def admit_in(
        self,
        conn,
        *,
        binding_id: str,
        principal_ref: str,
        argument_bytes: int,
    ) -> tuple[int | None, str | None]:
        """Serialize admission across Hosts and consume one principal token."""
        head = await self._head_in(conn)
        saved = await self._revision_in(conn, head["revision"], required=False)
        if saved is not None and saved["values_payload_state"] != "ACTIVE":
            return head["revision"], "BUSINESS_COMMAND_LIMITS_UNAVAILABLE"
        values = saved["values"] if saved else DEFAULT_COMMAND_LIMITS
        if argument_bytes > values["max_argument_bytes"]:
            return head["revision"] or None, "BUSINESS_COMMAND_ARGUMENT_TOO_LARGE"
        counts = await (
            await conn.execute(
                "SELECT count(*) AS bot_count,"
                "count(*) FILTER (WHERE binding_id=%s) AS binding_count,"
                "count(*) FILTER (WHERE principal_ref=%s) AS principal_count "
                "FROM public.bot_business_command_requests WHERE bot_id=%s "
                "AND status IN ('PENDING','CLAIMED')",
                (binding_id, principal_ref, self.store.bot_id),
            )
        ).fetchone()
        for count_key, limit_key, reason in (
            ("bot_count", "max_pending_bot", "BUSINESS_COMMAND_BOT_QUEUE_FULL"),
            ("binding_count", "max_pending_binding", "BUSINESS_COMMAND_BINDING_QUEUE_FULL"),
            ("principal_count", "max_pending_principal", "BUSINESS_COMMAND_PRINCIPAL_QUEUE_FULL"),
        ):
            if counts[count_key] >= values[limit_key]:
                return head["revision"] or None, reason
        now = await self.store._now(conn)
        bucket = await (
            await conn.execute(
                "SELECT * FROM public.bot_command_principal_buckets "
                "WHERE bot_id=%s AND principal_ref=%s FOR UPDATE",
                (self.store.bot_id, principal_ref),
            )
        ).fetchone()
        burst = Decimal(values["principal_burst"])
        if bucket is None:
            tokens = burst
            await conn.execute(
                "INSERT INTO public.bot_command_principal_buckets("
                "bot_id,principal_ref,tokens,updated_at) VALUES (%s,%s,%s,%s)",
                (self.store.bot_id, principal_ref, burst - 1, now),
            )
            return head["revision"] or None, None
        elapsed = Decimal(str(max(0.0, (now - bucket["updated_at"]).total_seconds())))
        tokens = min(
            burst,
            Decimal(bucket["tokens"]) + elapsed / Decimal(values["principal_refill_seconds"]),
        )
        admitted = tokens >= 1
        await conn.execute(
            "UPDATE public.bot_command_principal_buckets SET tokens=%s,updated_at=%s "
            "WHERE bot_id=%s AND principal_ref=%s",
            (
                tokens - 1 if admitted else tokens,
                now,
                self.store.bot_id,
                principal_ref,
            ),
        )
        return (
            head["revision"] or None,
            None if admitted else "BUSINESS_COMMAND_RATE_LIMITED",
        )

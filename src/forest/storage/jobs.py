"""Durable schedule definitions, delegated occurrences and fenced internal ingress."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import digest_json
from forest.schedules import JobSettings, aware, expand_schedule, next_occurrence, preview
from forest.storage.bindings import _clone
from forest.storage.control import BotControl


def _validate(payload, required, optional=()):
    if set(payload) - set(required) - set(optional) or not set(required) <= set(payload):
        raise ForestError("JOB_INVALID", "Job request fields differ", 422)
    for key in ("job_id", "occurrence_id", "plan_id", "plan_digest", "reason"):
        if key in payload and (
            not isinstance(payload[key], str) or not 1 <= len(payload[key]) <= 256
        ):
            raise ForestError(
                "JOB_INVALID", "Job identifiers and reasons must be bounded text", 422
            )
    if "expected_revision" in payload and (
        type(payload["expected_revision"]) is not int
        or not 0 <= payload["expected_revision"] < 2**63
    ):
        raise ForestError("JOB_INVALID", "Job revision must be a nonnegative integer", 422)


@dataclass(frozen=True)
class JobClaim:
    bot_id: str
    occurrence_id: str
    job_id: str
    job_revision: int
    delegation_epoch: int
    lease_epoch: int
    owner_id: str
    host_incarnation: str
    lease_until: datetime
    conversation_id: str
    binding_id: str
    values: dict
    delegation: dict


class JobStore:
    def __init__(self, messages, *, control=None):
        self.messages, self.store = messages, messages.store
        self.control = control or BotControl(self.store)

    @staticmethod
    async def cancel_revoked_triggers_in(conn, bot_id, conversation_id):
        """Caller already owns runtime and conversation; leave CLAIMED Runs alone."""
        rows = await (
            await conn.execute(
                "UPDATE public.bot_conversation_triggers t SET status='CANCELLED',"
                "terminal_reason='JOB_DELEGATION_REVOKED' WHERE t.bot_id=%s "
                "AND t.conversation_id=%s AND t.status='PENDING' "
                "AND EXISTS(SELECT 1 FROM public.bot_job_occurrences o WHERE o.bot_id=t.bot_id AND o.trigger_id=t.trigger_id) "
                "AND NOT public.forest_job_trigger_authorized(t.bot_id,t.trigger_id) RETURNING trigger_id",
                (bot_id, conversation_id),
            )
        ).fetchall()
        if rows:
            await conn.execute(
                "UPDATE public.bot_job_occurrences SET status='CANCELLED',revision=revision+1,"
                "error_code='JOB_DELEGATION_REVOKED',completed_at=clock_timestamp() "
                "WHERE bot_id=%s AND trigger_id=ANY(%s) AND status='ENQUEUED'",
                (bot_id, [r["trigger_id"] for r in rows]),
            )
            await conn.execute(
                "UPDATE public.bot_conversations SET mailbox_revision=mailbox_revision+1 "
                "WHERE bot_id=%s AND conversation_id=%s",
                (bot_id, conversation_id),
            )
        return len(rows)

    async def _head(self, conn, job_id, *, create=False):
        if create:
            await conn.execute(
                "INSERT INTO public.bot_jobs(bot_id,job_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (self.store.bot_id, job_id),
            )
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_jobs WHERE bot_id=%s AND job_id=%s FOR UPDATE",
                (self.store.bot_id, job_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("JOB_NOT_FOUND", "Job does not belong to this Bot", 404)
        return row

    async def _definition(self, conn, job_id, revision):
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_job_revisions WHERE bot_id=%s AND job_id=%s AND revision=%s",
                (self.store.bot_id, job_id, revision),
            )
        ).fetchone()
        if row is None:
            raise ForestError("JOB_NOT_CONFIGURED", "Apply a Job definition first", 409)
        return row

    async def _host(self, conn, lease, *, data=False):
        runtime = await self.store._runtime(conn)
        await self.store._host(conn, lease, runtime)
        if data and not runtime["data_admission"]:
            raise ForestError("DATA_ADMISSION_CLOSED", "Jobs wait while this Bot is paused", 409)
        return runtime

    @staticmethod
    async def authorize_shell_scope_in(conn, bot_id, conversation_id, payload):
        if "job_id" in payload:
            row = await (
                await conn.execute(
                    "SELECT conversation_id FROM public.bot_jobs WHERE bot_id=%s AND job_id=%s",
                    (bot_id, payload["job_id"]),
                )
            ).fetchone()
        elif "occurrence_id" in payload:
            row = await (
                await conn.execute(
                    "SELECT j.conversation_id FROM public.bot_jobs j JOIN public.bot_job_occurrences o USING(bot_id,job_id) WHERE o.bot_id=%s AND o.occurrence_id=%s",
                    (bot_id, payload["occurrence_id"]),
                )
            ).fetchone()
        else:
            raise ForestError("JOB_SCOPE_DENIED", "A group command requires one explicit Job", 403)
        if row is None or row["conversation_id"] != conversation_id:
            raise ForestError(
                "JOB_SCOPE_DENIED", "Group commands can manage only their own conversation", 403
            )

    async def read(self, ticket, operation_id, payload=None):
        payload = _clone({} if payload is None else payload)
        _validate(payload, (), ("job_id",))
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "job.read", operation_id, payload)
            rows = await (
                await conn.execute(
                    "SELECT j.*,r.values,r.delegation,c.binding_id,c.platform,c.channel_kind,c.channel_id,c.thread_id,c.group_source_id "
                    "FROM public.bot_jobs j JOIN public.bot_job_revisions r ON r.bot_id=j.bot_id AND r.job_id=j.job_id AND r.revision=j.definition_revision "
                    "JOIN public.bot_conversations c ON c.bot_id=j.bot_id AND c.conversation_id=j.conversation_id "
                    "WHERE j.bot_id=%s AND (%s::text IS NULL OR j.job_id=%s) "
                    "AND (%s::text IS NULL OR j.conversation_id=%s) ORDER BY j.created_at,j.job_id LIMIT 200",
                    (
                        self.store.bot_id,
                        payload.get("job_id"),
                        payload.get("job_id"),
                        claims.get("source_conversation_id"),
                        claims.get("source_conversation_id"),
                    ),
                )
            ).fetchall()
            occurrences = await (
                await conn.execute(
                    "SELECT o.*,t.status AS trigger_status,t.claimed_run_id AS run_id,t.answered_reply_id AS reply_id,"
                    "r.status AS run_status,(SELECT string_agg(DISTINCT p.state,',' ORDER BY p.state) FROM public.bot_send_parts p "
                    "JOIN public.bot_conversation_replies q ON q.bot_id=p.bot_id AND q.delivery->>'intent_id'=p.intent_id "
                    "WHERE q.bot_id=o.bot_id AND q.reply_id=t.answered_reply_id) AS delivery_state "
                    "FROM public.bot_job_occurrences o LEFT JOIN public.bot_conversation_triggers t ON t.bot_id=o.bot_id AND t.trigger_id=o.trigger_id "
                    "LEFT JOIN public.bot_conversation_runs r ON r.bot_id=t.bot_id AND r.run_id=t.claimed_run_id "
                    "JOIN public.bot_jobs j ON j.bot_id=o.bot_id AND j.job_id=o.job_id "
                    "WHERE o.bot_id=%s AND (%s::text IS NULL OR o.job_id=%s) "
                    "AND (%s::text IS NULL OR j.conversation_id=%s) ORDER BY o.created_at DESC,o.occurrence_id LIMIT 200",
                    (
                        self.store.bot_id,
                        payload.get("job_id"),
                        payload.get("job_id"),
                        claims.get("source_conversation_id"),
                        claims.get("source_conversation_id"),
                    ),
                )
            ).fetchall()
            now = await self.store._now(conn)
            jobs, previews = [], {}
            for row in rows:
                conversation = {
                    key: row[key]
                    for key in (
                        "conversation_id",
                        "binding_id",
                        "platform",
                        "channel_kind",
                        "channel_id",
                        "thread_id",
                        "group_source_id",
                    )
                }
                jobs.append(
                    {
                        **row,
                        "values": {**row["values"], "enabled": row["enabled"]},
                        "conversation": conversation,
                    }
                )
                previews[row["job_id"]] = (
                    preview(row["values"]["schedule"], now, limit=3) if row["enabled"] else []
                )
            heads = {row["job_id"]: row for row in rows}
            valid_heads = set()
            runtime = await self.store._runtime(conn)
            for head in rows:
                try:
                    await self._delegation(conn, head, runtime)
                    valid_heads.add(head["job_id"])
                except ForestError:
                    pass
            for row in occurrences:
                head = heads.get(row["job_id"])
                row["replay_allowed"] = bool(
                    head
                    and row["status"] == "FAILED"
                    and row["event_id"] is None
                    and row["expires_at"] > now
                    and row["delegation_epoch"] == head["delegation_epoch"]
                    and row["job_id"] in valid_heads
                )
            await self.control._time(conn, claims)
            return {"jobs": jobs, "occurrences": occurrences, "next_occurrences": previews}

    async def plan(self, ticket, operation_id, payload):
        payload = _clone(payload)
        _validate(payload, ("job_id", "expected_revision", "values"))
        try:
            values = JobSettings.model_validate(payload["values"]).model_dump()
        except ValueError:
            raise ForestError(
                "JOB_INVALID", "Job schedule and delegation settings are invalid", 422
            ) from None
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "job.plan", operation_id, payload)
            head = await self._head(conn, payload["job_id"], create=True)
            replay = await self.control._replay(conn, claims)
            if replay is not None:
                await self.control._time(conn, claims)
                return replay
            if head["revision"] != payload["expected_revision"]:
                raise ForestError(
                    "REVISION_CONFLICT", "Job changed; reload the current definition", 409
                )
            conversation = await self.messages._locate(conn, values["conversation_id"])
            if head["conversation_id"] not in {None, values["conversation_id"]}:
                raise ForestError(
                    "JOB_TARGET_IMMUTABLE", "Create a different Job to change its conversation", 409
                )
            now = await self.store._now(conn)
            if aware(values["delegation_expires_at"]) <= now:
                raise ForestError(
                    "JOB_DELEGATION_EXPIRED", "Delegation must expire in the future", 422
                )
            before = (
                await self._definition(conn, head["job_id"], head["definition_revision"])
                if head["definition_revision"] is not None
                else None
            )
            plan_id = str(uuid4())
            digest = digest_json(
                {
                    "job_id": head["job_id"],
                    "values": values,
                    "expected_revision": head["revision"],
                    "subject_id": claims["subject_id"],
                    "issuer_id": claims["issuer_id"],
                }
            )
            await conn.execute(
                "INSERT INTO public.bot_job_plans(bot_id,job_id,plan_id,expected_revision,subject_id,issuer_id,values,plan_digest,expires_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp()+interval '10 minutes')",
                (
                    self.store.bot_id,
                    head["job_id"],
                    plan_id,
                    head["revision"],
                    claims["subject_id"],
                    claims["issuer_id"],
                    Jsonb(values),
                    digest,
                ),
            )
            result = {
                "job_id": head["job_id"],
                "expected_revision": head["revision"],
                "plan_id": plan_id,
                "plan_digest": digest,
                "values": values,
                "before_values": (
                    {**before["values"], "enabled": head["enabled"]} if before else None
                ),
                "next_occurrences": preview(values["schedule"], now),
                "conversation": conversation,
            }
            await self.control._record(conn, claims, result)
            await self.control._time(conn, claims)
            return result

    async def apply(self, ticket, operation_id, payload):
        payload = _clone(payload)
        _validate(payload, ("job_id", "expected_revision", "plan_id", "plan_digest"))
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "job.apply", operation_id, payload, payload["plan_digest"]
            )
            head = await self._head(conn, payload["job_id"])
            replay = await self.control._replay(conn, claims)
            if replay is not None:
                await self.control._time(conn, claims)
                return replay
            plan = await (
                await conn.execute(
                    "SELECT * FROM public.bot_job_plans WHERE bot_id=%s AND plan_id=%s",
                    (self.store.bot_id, payload["plan_id"]),
                )
            ).fetchone()
            if (
                not plan
                or any(
                    plan[key] != payload[key]
                    for key in ("job_id", "expected_revision", "plan_digest")
                )
                or any(plan[key] != claims[key] for key in ("issuer_id", "subject_id"))
            ):
                raise ForestError(
                    "PLAN_CONFLICT", "Job plan does not match this request and principal", 409
                )
            if head["revision"] != payload["expected_revision"]:
                raise ForestError("REVISION_CONFLICT", "Job changed after the preview", 409)
            now = await self.store._now(conn)
            if plan["expires_at"] <= now or aware(plan["values"]["delegation_expires_at"]) <= now:
                raise ForestError("PLAN_EXPIRED", "Job plan or delegation expired", 409)
            await self.messages._locate(conn, plan["values"]["conversation_id"])
            revision, epoch = head["revision"] + 1, head["delegation_epoch"] + 1
            delegation = {
                key: claims[key]
                for key in (
                    "issuer_id",
                    "subject_id",
                    "key_revision",
                    "account_revision",
                    "policy_revision",
                )
            }
            delegation.update(
                {
                    "delegation_id": "job-delegation:"
                    + digest_json([self.store.bot_id, head["job_id"], revision]),
                    "delegation_epoch": epoch,
                    "origin_kind": claims.get("origin_kind", "web"),
                    "principal_ref": "delegation:" + claims["subject_id"],
                    "expires_at": plan["values"]["delegation_expires_at"],
                    "capabilities": ["conversation.trigger"],
                    "tool_capabilities": [],
                    "schedule_algorithm": "forest-calendar-v1",
                }
            )
            for key in ("source_binding_id", "source_principal"):
                if key in claims:
                    delegation[key] = claims[key]
            due = next_occurrence(plan["values"]["schedule"], now - timedelta(microseconds=1))
            await conn.execute(
                "INSERT INTO public.bot_job_revisions(bot_id,job_id,revision,values,delegation,definition_digest,operation_id) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    head["job_id"],
                    revision,
                    Jsonb(plan["values"]),
                    Jsonb(delegation),
                    digest_json(plan["values"]),
                    operation_id,
                ),
            )
            await conn.execute(
                "UPDATE public.bot_jobs SET revision=%s,definition_revision=%s,conversation_id=%s,enabled=%s,delegation_epoch=%s,next_due_at=%s,emitted_count=0,updated_at=clock_timestamp() WHERE bot_id=%s AND job_id=%s",
                (
                    revision,
                    revision,
                    plan["values"]["conversation_id"],
                    plan["values"]["enabled"],
                    epoch,
                    due,
                    self.store.bot_id,
                    head["job_id"],
                ),
            )
            await conn.execute(
                "UPDATE public.bot_job_occurrences SET status='CANCELLED',revision=revision+1,error_code='JOB_DEFINITION_REPLACED',completed_at=clock_timestamp() WHERE bot_id=%s AND job_id=%s AND status IN ('QUEUED','CLAIMED')",
                (self.store.bot_id, head["job_id"]),
            )
            result = {
                "job_id": head["job_id"],
                "revision": revision,
                "status": "APPLIED",
                "delegation": delegation,
                "next_due_at": due.isoformat() if due else None,
            }
            await self.control._record(conn, claims, result)
            await self.control._time(conn, claims)
            if min(plan["expires_at"], aware(delegation["expires_at"])) <= await self.store._now(
                conn
            ):
                raise ForestError("PLAN_EXPIRED", "Job plan expired while committing", 409)
            return result

    async def _delegation(self, conn, definition, runtime):
        grant = definition["delegation"]
        row = await (
            await conn.execute(
                "SELECT enabled,key_revision,actions FROM public.bot_web_issuers WHERE bot_id=%s AND issuer_id=%s",
                (self.store.bot_id, grant["issuer_id"]),
            )
        ).fetchone()
        if (
            not row
            or not row["enabled"]
            or row["key_revision"] != grant["key_revision"]
            or "job.run" not in row["actions"]
            or grant["policy_revision"] != runtime["policy_revision"]
        ):
            raise ForestError(
                "JOB_DELEGATION_REVOKED", "Job delegation no longer has current authority", 403
            )
        if aware(grant["expires_at"]) <= await self.store._now(conn):
            raise ForestError("JOB_DELEGATION_EXPIRED", "Job delegation expired", 409)
        return grant

    async def _emit(self, conn, head, definition, scheduled_for, *, origin, identity):
        occurrence_id = "job-occurrence:" + digest_json(
            [self.store.bot_id, head["job_id"], head["definition_revision"], origin, identity]
        )
        existing = await (
            await conn.execute(
                "SELECT * FROM public.bot_job_occurrences WHERE bot_id=%s AND occurrence_id=%s",
                (self.store.bot_id, occurrence_id),
            )
        ).fetchone()
        if existing:
            return existing
        if head["emitted_count"] >= definition["values"]["max_runs"]:
            raise ForestError("JOB_BUDGET_EXHAUSTED", "Job occurrence limit reached", 409)
        queued = await (
            await conn.execute(
                "SELECT count(*) AS n FROM public.bot_job_occurrences WHERE bot_id=%s AND job_id=%s AND status IN ('QUEUED','CLAIMED')",
                (self.store.bot_id, head["job_id"]),
            )
        ).fetchone()
        if queued["n"] >= 64:
            raise ForestError("JOB_BACKPRESSURE", "Job has too many queued occurrences", 429)
        result = await (
            await conn.execute(
                "INSERT INTO public.bot_job_occurrences(bot_id,occurrence_id,job_id,job_revision,delegation_epoch,origin,scheduled_for,expires_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                (
                    self.store.bot_id,
                    occurrence_id,
                    head["job_id"],
                    head["definition_revision"],
                    head["delegation_epoch"],
                    origin,
                    scheduled_for,
                    aware(definition["delegation"]["expires_at"]),
                ),
            )
        ).fetchone()
        head["emitted_count"] += 1
        await conn.execute(
            "UPDATE public.bot_jobs SET emitted_count=%s WHERE bot_id=%s AND job_id=%s",
            (head["emitted_count"], self.store.bot_id, head["job_id"]),
        )
        return result

    async def run_now(self, ticket, operation_id, payload):
        payload = _clone(payload)
        _validate(payload, ("job_id", "expected_revision"))
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "job.run", operation_id, payload)
            head = await self._head(conn, payload["job_id"])
            replay = await self.control._replay(conn, claims)
            if replay is not None:
                await self.control._time(conn, claims)
                return replay
            if head["revision"] != payload["expected_revision"]:
                raise ForestError("REVISION_CONFLICT", "Job definition changed", 409)
            definition = await self._definition(conn, head["job_id"], head["definition_revision"])
            await self._delegation(conn, definition, await self.store._runtime(conn))
            occurrence = await self._emit(
                conn,
                head,
                definition,
                await self.store._now(conn),
                origin="MANUAL",
                identity=operation_id,
            )
            result = {
                "job_id": head["job_id"],
                "occurrence_id": occurrence["occurrence_id"],
                "revision": occurrence["revision"],
                "status": occurrence["status"],
            }
            await self.control._record(conn, claims, result)
            await self.control._time(conn, claims)
            if occurrence["expires_at"] <= await self.store._now(conn):
                raise ForestError(
                    "JOB_DELEGATION_EXPIRED", "Job delegation expired before manual acceptance", 409
                )
            return result

    async def expand_due(self, lease, *, limit=32):
        if type(limit) is not int or not 1 <= limit <= 128:
            raise ValueError("Job scan limit")
        async with self.store.pool.connection() as conn, conn.transaction():
            runtime = await self._host(conn, lease, data=True)
            rows = await (
                await conn.execute(
                    "SELECT * FROM public.bot_jobs WHERE bot_id=%s AND enabled AND next_due_at<=clock_timestamp() ORDER BY next_due_at,job_id LIMIT %s FOR UPDATE",
                    (self.store.bot_id, limit),
                )
            ).fetchall()
            count = 0
            for head in rows:
                definition = await self._definition(
                    conn, head["job_id"], head["definition_revision"]
                )
                now = await self.store._now(conn)
                try:
                    await self._delegation(conn, definition, runtime)
                except ForestError:
                    await conn.execute(
                        "UPDATE public.bot_jobs SET next_due_at=NULL,enabled=false,revision=revision+1,updated_at=clock_timestamp() WHERE bot_id=%s AND job_id=%s",
                        (self.store.bot_id, head["job_id"]),
                    )
                    continue
                expanded = expand_schedule(definition["values"], head["next_due_at"], now)
                selected = []
                for instant in expanded["occurrences"]:
                    try:
                        item = await self._emit(
                            conn,
                            head,
                            definition,
                            instant,
                            origin="SCHEDULE",
                            identity=instant.isoformat(),
                        )
                    except ForestError as exc:
                        if exc.code not in {"JOB_BUDGET_EXHAUSTED", "JOB_BACKPRESSURE"}:
                            raise
                        break
                    selected.append(item["occurrence_id"])
                    count += 1
                await conn.execute(
                    "INSERT INTO public.bot_job_expansions(bot_id,expansion_id,job_id,job_revision,from_due_at,through_at,selected_occurrences,misfired) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        self.store.bot_id,
                        str(uuid4()),
                        head["job_id"],
                        head["definition_revision"],
                        head["next_due_at"],
                        now,
                        Jsonb(selected),
                        expanded["misfired"],
                    ),
                )
                due = (
                    expanded["next_due_at"]
                    if head["emitted_count"] < definition["values"]["max_runs"]
                    else None
                )
                await conn.execute(
                    "UPDATE public.bot_jobs SET next_due_at=%s WHERE bot_id=%s AND job_id=%s",
                    (due, self.store.bot_id, head["job_id"]),
                )
            await self.store._host(conn, lease, runtime)
            return count

    async def claim_pending(self, lease, owner_id, *, ttl_seconds=30):
        if (
            not isinstance(owner_id, str)
            or not 1 <= len(owner_id) <= 256
            or not 0 < ttl_seconds <= 120
        ):
            raise ValueError("Job owner or lease duration")
        async with self.store.pool.connection() as conn, conn.transaction():
            runtime = await self._host(conn, lease, data=True)
            rows = await (
                await conn.execute(
                    "SELECT o.occurrence_id,o.job_id FROM public.bot_job_occurrences o JOIN public.bot_jobs j USING(bot_id,job_id) "
                    "JOIN public.bot_job_revisions d ON d.bot_id=o.bot_id AND d.job_id=o.job_id AND d.revision=o.job_revision "
                    "WHERE o.bot_id=%s AND o.next_attempt_at<=clock_timestamp() AND o.expires_at>clock_timestamp() "
                    "AND public.forest_job_occurrence_authorized(o.bot_id,o.occurrence_id) "
                    "AND o.delegation_epoch=j.delegation_epoch AND (o.status='QUEUED' OR (o.status='CLAIMED' AND "
                    "(o.lease_until<=clock_timestamp() OR o.host_incarnation<>%s))) "
                    "AND (SELECT count(*) FROM public.bot_job_occurrences a WHERE a.bot_id=o.bot_id AND a.job_id=o.job_id "
                    "AND a.occurrence_id<>o.occurrence_id AND (a.status='ENQUEUED' OR (a.status='CLAIMED' AND a.lease_until>clock_timestamp() AND a.host_incarnation=%s))) < (d.values->>'max_overlap')::int "
                    "ORDER BY o.scheduled_for,o.occurrence_id LIMIT 1",
                    (self.store.bot_id, lease.incarnation, lease.incarnation),
                )
            ).fetchall()
            if not rows:
                await self.store._host(conn, lease, runtime)
                return None
            head = await self._head(conn, rows[0]["job_id"])
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_job_occurrences WHERE bot_id=%s AND occurrence_id=%s FOR UPDATE",
                    (self.store.bot_id, rows[0]["occurrence_id"]),
                )
            ).fetchone()
            definition = await self._definition(conn, head["job_id"], row["job_revision"])
            await self._delegation(conn, definition, runtime)
            target = await self.messages._locate(conn, definition["values"]["conversation_id"])
            row = await (
                await conn.execute(
                    "UPDATE public.bot_job_occurrences SET status='CLAIMED',revision=revision+1,lease_epoch=lease_epoch+1,owner_id=%s,host_incarnation=%s,lease_until=clock_timestamp()+%s*interval '1 second',attempt_count=attempt_count+1 WHERE bot_id=%s AND occurrence_id=%s RETURNING *",
                    (
                        owner_id,
                        lease.incarnation,
                        ttl_seconds,
                        self.store.bot_id,
                        row["occurrence_id"],
                    ),
                )
            ).fetchone()
            await self.store._host(conn, lease, runtime)
            if row["lease_until"] <= await self.store._now(conn):
                raise ForestError("STALE_JOB", "Job lease expired while claiming", 409)
            return JobClaim(
                **{
                    key: row[key]
                    for key in (
                        "bot_id",
                        "occurrence_id",
                        "job_id",
                        "job_revision",
                        "delegation_epoch",
                        "lease_epoch",
                        "owner_id",
                        "host_incarnation",
                        "lease_until",
                    )
                },
                conversation_id=definition["values"]["conversation_id"],
                binding_id=target["binding_id"],
                values=definition["values"],
                delegation=definition["delegation"],
            )

    @staticmethod
    def authorization_request(claim):
        return {
            "action": "job.run",
            "operation_id": "job-dispatch:" + claim.occurrence_id + ":" + str(claim.lease_epoch),
            "payload": {
                "job_id": claim.job_id,
                "job_revision": claim.job_revision,
                "occurrence_id": claim.occurrence_id,
                "delegation_id": claim.delegation["delegation_id"],
                "delegation_epoch": claim.delegation_epoch,
                "lease_epoch": claim.lease_epoch,
            },
        }

    async def _owned(self, conn, lease, claim):
        head = await self._head(conn, claim.job_id)
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_job_occurrences WHERE bot_id=%s AND occurrence_id=%s FOR UPDATE",
                (self.store.bot_id, claim.occurrence_id),
            )
        ).fetchone()
        if (
            row is None
            or claim.bot_id != self.store.bot_id
            or claim.host_incarnation != lease.incarnation
            or any(
                row[key] != getattr(claim, key)
                for key in (
                    "job_id",
                    "job_revision",
                    "delegation_epoch",
                    "lease_epoch",
                    "owner_id",
                    "host_incarnation",
                )
            )
        ):
            raise ForestError("STALE_JOB", "Job occurrence belongs to another execution owner", 409)
        if row["event_id"] is not None:
            return head, row
        if (
            row["status"] != "CLAIMED"
            or row["lease_until"] <= await self.store._now(conn)
            or head["delegation_epoch"] != row["delegation_epoch"]
        ):
            raise ForestError("STALE_JOB", "Job occurrence lease or delegation changed", 409)
        return head, row

    async def dispatch(self, lease, claim, *, authorization=None):
        if authorization is None:
            raise ForestError(
                "JOB_AUTHORIZATION_REQUIRED", "A current delegation proof is required", 503
            )
        authorization = _clone(authorization)
        request = self.authorization_request(claim)
        async with self.store.pool.connection() as conn, conn.transaction():
            binding = await self.messages.bindings._guard(conn, lease, claim.binding_id, data=True)
            conversation = await self.messages._conversation(conn, claim.conversation_id)
            if conversation["binding_id"] != claim.binding_id:
                raise ForestError("JOB_SCOPE_DENIED", "Job target Binding differs", 403)
            claims = await BotControl(self.store)._guard(
                conn, authorization, "job.run", request["operation_id"], request["payload"]
            )
            head, row = await self._owned(conn, lease, claim)
            definition = await self._definition(conn, head["job_id"], row["job_revision"])
            grant = await self._delegation(conn, definition, binding["_runtime"])
            if (
                any(
                    claims[key] != grant[key]
                    for key in (
                        "issuer_id",
                        "subject_id",
                        "key_revision",
                        "account_revision",
                        "policy_revision",
                    )
                )
                or request["payload"]["delegation_id"] != grant["delegation_id"]
            ):
                raise ForestError(
                    "JOB_DELEGATION_REVOKED",
                    "Dispatch proof differs from the immutable delegation",
                    403,
                )
            if row["event_id"] is not None:
                await self.messages.bindings._final_guard(conn, lease, binding, data=True)
                await BotControl(self.store)._time(conn, claims)
                return {
                    "occurrence_id": row["occurrence_id"],
                    "event_id": row["event_id"],
                    "trigger_id": row["trigger_id"],
                    "status": row["status"],
                    "replayed": True,
                }
            if definition["values"]["conversation_id"] != claim.conversation_id:
                raise ForestError("JOB_SCOPE_DENIED", "Job target conversation differs", 403)
            receipt = await self.messages.enqueue_job_trigger_in(
                conn, lease, row, binding, definition
            )
            await conn.execute(
                "UPDATE public.bot_job_occurrences SET status='ENQUEUED',revision=revision+1,event_id=%s,trigger_id=%s,dispatched_at=clock_timestamp(),lease_until=NULL,error_code=NULL WHERE bot_id=%s AND occurrence_id=%s",
                (
                    receipt["event_id"],
                    receipt["trigger_id"],
                    self.store.bot_id,
                    row["occurrence_id"],
                ),
            )
            await self.messages.bindings._final_guard(conn, lease, binding, data=True)
            await BotControl(self.store)._time(conn, claims)
            if min(
                row["lease_until"], row["expires_at"], aware(grant["expires_at"])
            ) <= await self.store._now(conn):
                raise ForestError("STALE_JOB", "Job permission expired before durable ingress", 409)
            return {"occurrence_id": row["occurrence_id"], "status": "ENQUEUED", **receipt}

    async def defer(self, lease, claim, error_code, *, delay_seconds=30):
        if (
            not isinstance(error_code, str)
            or not 1 <= len(error_code) <= 256
            or not 1 <= delay_seconds <= 3600
        ):
            raise ValueError("Job retry policy")
        async with self.store.pool.connection() as conn, conn.transaction():
            runtime = await self._host(conn, lease)
            _, row = await self._owned(conn, lease, claim)
            if row["event_id"] is None:
                await conn.execute(
                    "UPDATE public.bot_job_occurrences SET status='QUEUED',revision=revision+1,lease_until=NULL,next_attempt_at=clock_timestamp()+%s*interval '1 second',error_code=%s WHERE bot_id=%s AND occurrence_id=%s",
                    (delay_seconds, error_code, self.store.bot_id, row["occurrence_id"]),
                )
            await self.store._host(conn, lease, runtime)
            if row["event_id"] is None and row["lease_until"] <= await self.store._now(conn):
                raise ForestError("STALE_JOB", "Job lease expired before deferral", 409)

    async def fail(self, lease, claim, error_code):
        """Definitive failure before ingress; network/proof outages use defer."""
        if not isinstance(error_code, str) or not 1 <= len(error_code) <= 256:
            raise ValueError("Job error code")
        async with self.store.pool.connection() as conn, conn.transaction():
            runtime = await self._host(conn, lease)
            _, row = await self._owned(conn, lease, claim)
            if row["event_id"] is not None:
                raise ForestError(
                    "JOB_ALREADY_ENQUEUED", "An accepted trigger cannot be failed for replay", 409
                )
            await conn.execute(
                "UPDATE public.bot_job_occurrences SET status='FAILED',revision=revision+1,"
                "error_code=%s,completed_at=clock_timestamp(),lease_until=NULL "
                "WHERE bot_id=%s AND occurrence_id=%s",
                (error_code, self.store.bot_id, row["occurrence_id"]),
            )
            await self.store._host(conn, lease, runtime)
            if row["lease_until"] <= await self.store._now(conn):
                raise ForestError("STALE_JOB", "Job lease expired before failure checkpoint", 409)

    async def reconcile(self, lease, *, limit=128):
        if type(limit) is not int or not 1 <= limit <= 512:
            raise ValueError("Job reconcile limit")
        async with self.store.pool.connection() as conn, conn.transaction():
            runtime = await self._host(conn, lease)
            conversations = await (
                await conn.execute(
                    "SELECT DISTINCT t.conversation_id FROM public.bot_job_occurrences o "
                    "JOIN public.bot_conversation_triggers t USING(bot_id,trigger_id) "
                    "WHERE o.bot_id=%s AND o.status='ENQUEUED' AND t.status='PENDING' "
                    "AND NOT public.forest_job_trigger_authorized(t.bot_id,t.trigger_id) "
                    "ORDER BY t.conversation_id LIMIT %s",
                    (self.store.bot_id, limit),
                )
            ).fetchall()
            cancelled = 0
            for target in conversations:
                await self.messages._conversation(conn, target["conversation_id"])
                cancelled += await self.cancel_revoked_triggers_in(
                    conn, self.store.bot_id, target["conversation_id"]
                )
            rows = await (
                await conn.execute(
                    "SELECT o.*,t.status AS trigger_status FROM public.bot_job_occurrences o LEFT JOIN public.bot_conversation_triggers t ON t.bot_id=o.bot_id AND t.trigger_id=o.trigger_id WHERE o.bot_id=%s AND ((o.status IN ('QUEUED','CLAIMED') AND NOT public.forest_job_occurrence_authorized(o.bot_id,o.occurrence_id)) OR (o.status='ENQUEUED' AND t.status IN ('ANSWERED','FAILED','CANCELLED'))) ORDER BY o.created_at,o.occurrence_id LIMIT %s FOR UPDATE OF o",
                    (self.store.bot_id, limit),
                )
            ).fetchall()
            for row in rows:
                status = {
                    "ANSWERED": "SUCCEEDED",
                    "FAILED": "FAILED",
                    "CANCELLED": "CANCELLED",
                }.get(
                    row["trigger_status"],
                    "EXPIRED" if row["expires_at"] <= await self.store._now(conn) else "CANCELLED",
                )
                await conn.execute(
                    "UPDATE public.bot_job_occurrences SET status=%s,revision=revision+1,completed_at=clock_timestamp() WHERE bot_id=%s AND occurrence_id=%s",
                    (status, self.store.bot_id, row["occurrence_id"]),
                )
            await self.store._host(conn, lease, runtime)
            return len(rows) + cancelled

    async def cancel(self, ticket, operation_id, payload):
        payload = _clone(payload)
        _validate(payload, ("job_id", "expected_revision", "scope", "reason"))
        if payload["scope"] not in {"future", "pending", "all"}:
            raise ForestError("JOB_INVALID", "Unknown Job cancellation scope", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "job.cancel", operation_id, payload)
            head = await self._head(conn, payload["job_id"])
            replay = await self.control._replay(conn, claims)
            if replay is not None:
                await self.control._time(conn, claims)
                return replay
            if head["revision"] != payload["expected_revision"]:
                raise ForestError("REVISION_CONFLICT", "Job cancellation used an old revision", 409)
            if head["conversation_id"]:
                await self.messages._conversation(conn, head["conversation_id"])
            rows = await (
                await conn.execute(
                    "SELECT o.*,t.status AS trigger_status,t.claimed_run_id,a.checkpoint_seq FROM public.bot_job_occurrences o LEFT JOIN public.bot_conversation_triggers t ON t.bot_id=o.bot_id AND t.trigger_id=o.trigger_id LEFT JOIN public.bot_agent_runs a ON a.bot_id=t.bot_id AND a.run_id=t.claimed_run_id WHERE o.bot_id=%s AND o.job_id=%s AND o.status IN ('QUEUED','CLAIMED','ENQUEUED') ORDER BY o.occurrence_id FOR UPDATE OF o",
                    (self.store.bot_id, head["job_id"]),
                )
            ).fetchall()
            active = [
                {"run_id": row["claimed_run_id"], "expected_checkpoint_seq": row["checkpoint_seq"]}
                for row in rows
                if row["trigger_status"] == "CLAIMED"
            ]
            if payload["scope"] == "all" and active:
                raise ForestError(
                    "JOB_RUN_ALREADY_ACTIVE",
                    "Cancel the displayed Run explicitly; it may include other triggers",
                    409,
                    {"runs": active},
                )
            cancelled = []
            if payload["scope"] in {"pending", "all"}:
                for row in rows:
                    if row["trigger_status"] not in {None, "PENDING"}:
                        continue
                    if row["trigger_id"]:
                        await conn.execute(
                            "UPDATE public.bot_conversation_triggers SET status='CANCELLED',terminal_reason=%s WHERE bot_id=%s AND trigger_id=%s AND status='PENDING'",
                            (payload["reason"], self.store.bot_id, row["trigger_id"]),
                        )
                    await conn.execute(
                        "UPDATE public.bot_job_occurrences SET status='CANCELLED',revision=revision+1,error_code='JOB_CANCELLED',completed_at=clock_timestamp() WHERE bot_id=%s AND occurrence_id=%s",
                        (self.store.bot_id, row["occurrence_id"]),
                    )
                    cancelled.append(row["occurrence_id"])
            enabled = head["enabled"] and payload["scope"] != "future" and payload["scope"] != "all"
            await conn.execute(
                "UPDATE public.bot_jobs SET revision=revision+1,enabled=%s,updated_at=clock_timestamp() WHERE bot_id=%s AND job_id=%s",
                (enabled, self.store.bot_id, head["job_id"]),
            )
            result = {
                "job_id": head["job_id"],
                "revision": head["revision"] + 1,
                "scope": payload["scope"],
                "cancelled_occurrences": cancelled,
                "active_runs": active,
                "enabled": enabled,
            }
            await self.control._record(conn, claims, result)
            await self.control._time(conn, claims)
            return result

    async def replay(self, ticket, operation_id, payload):
        payload = _clone(payload)
        _validate(payload, ("occurrence_id", "expected_revision"))
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "job.replay", operation_id, payload)
            replay = await self.control._replay(conn, claims)
            if replay is not None:
                await self.control._time(conn, claims)
                return replay
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_job_occurrences WHERE bot_id=%s AND occurrence_id=%s FOR UPDATE",
                    (self.store.bot_id, payload["occurrence_id"]),
                )
            ).fetchone()
            if not row:
                raise ForestError(
                    "JOB_NOT_FOUND", "Job occurrence does not belong to this Bot", 404
                )
            head = await self._head(conn, row["job_id"])
            if (
                row["revision"] != payload["expected_revision"]
                or row["status"] != "FAILED"
                or row["event_id"] is not None
                or row["delegation_epoch"] != head["delegation_epoch"]
            ):
                raise ForestError(
                    "JOB_REPLAY_CONFLICT",
                    "Only an unchanged, never-enqueued failed occurrence can retry",
                    409,
                )
            await self._delegation(
                conn,
                await self._definition(conn, row["job_id"], row["job_revision"]),
                await self.store._runtime(conn),
            )
            await conn.execute(
                "UPDATE public.bot_job_occurrences SET status='QUEUED',revision=revision+1,error_code=NULL,next_attempt_at=clock_timestamp(),completed_at=NULL WHERE bot_id=%s AND occurrence_id=%s",
                (self.store.bot_id, row["occurrence_id"]),
            )
            result = {
                "occurrence_id": row["occurrence_id"],
                "revision": row["revision"] + 1,
                "status": "QUEUED",
            }
            await self.control._record(conn, claims, result)
            await self.control._time(conn, claims)
            if row["expires_at"] <= await self.store._now(conn):
                raise ForestError(
                    "JOB_DELEGATION_EXPIRED", "Job delegation expired before replay acceptance", 409
                )
            return result

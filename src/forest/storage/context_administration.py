"""Conversation-local Context inspection and safe-point compaction requests."""

from __future__ import annotations

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.control import BotControl


class ContextAdministration:
    def __init__(self, messages, *, control=None):
        self.messages = messages
        self.store = messages.store
        self.control = control or BotControl(self.store)

    @staticmethod
    def _conversation_id(claims):
        value = claims.get("source_conversation_id")
        if not isinstance(value, str) or not value:
            raise ForestError(
                "CONTEXT_SCOPE_REQUIRED", "Context control requires an origin conversation", 403
            )
        return value

    async def read(self, ticket, operation_id, payload):
        if payload != {}:
            raise ForestError("CONTEXT_CONTROL_INVALID", "Context inspection has no fields", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "context.read", operation_id, payload
            )
            conversation_id = self._conversation_id(claims)
            conversation = await (
                await conn.execute(
                    "SELECT c.*,s.status AS session_status,a.run_id,a.phase,a.checkpoint_seq "
                    "FROM public.bot_conversations c "
                    "LEFT JOIN public.bot_conversation_sessions s "
                    "ON s.bot_id=c.bot_id AND s.session_id=c.active_session_id "
                    "LEFT JOIN public.bot_agent_runs a "
                    "ON a.bot_id=s.bot_id AND a.run_id=s.active_run_id "
                    "WHERE c.bot_id=%s AND c.conversation_id=%s",
                    (self.store.bot_id, conversation_id),
                )
            ).fetchone()
            if conversation is None:
                raise ForestError("CONVERSATION_NOT_FOUND", "Conversation does not exist", 404)
            counts = await (
                await conn.execute(
                    "SELECT count(*) FILTER(WHERE visible) AS visible_units,"
                    "count(*) FILTER(WHERE NOT visible) AS archived_units,"
                    "COALESCE(sum(estimated_tokens) FILTER(WHERE visible),0) AS visible_tokens "
                    "FROM public.bot_context_units WHERE bot_id=%s AND conversation_id=%s",
                    (self.store.bot_id, conversation_id),
                )
            ).fetchone()
            triggers = await (
                await conn.execute(
                    "SELECT count(*) FILTER(WHERE status='PENDING') AS pending_triggers,"
                    "count(*) FILTER(WHERE status='CLAIMED') AS claimed_triggers "
                    "FROM public.bot_conversation_triggers "
                    "WHERE bot_id=%s AND conversation_id=%s",
                    (self.store.bot_id, conversation_id),
                )
            ).fetchone()
            requests = await (
                await conn.execute(
                    "SELECT request_id,kind,mode,expected_view_revision,status,applied_view_revision,"
                    "terminal_reason,requested_at,completed_at "
                    "FROM public.bot_context_management_requests "
                    "WHERE bot_id=%s AND conversation_id=%s "
                    "ORDER BY requested_at DESC,request_id DESC LIMIT 8",
                    (self.store.bot_id, conversation_id),
                )
            ).fetchall()
            await self.control._time(conn, claims)
            return {
                "conversation_id": conversation_id,
                "context_revision": conversation["context_revision"],
                "context_view_revision": conversation["context_view_revision"],
                "context_applied_seq": conversation["context_applied_seq"],
                "classified_through_seq": conversation["classified_through_seq"],
                "visible_units": counts["visible_units"],
                "archived_units": counts["archived_units"],
                "visible_tokens": counts["visible_tokens"],
                "token_budget": self.messages.context_token_budget,
                "pending_triggers": triggers["pending_triggers"],
                "claimed_triggers": triggers["claimed_triggers"],
                "session_status": conversation["session_status"],
                "run_id": conversation["run_id"],
                "run_phase": conversation["phase"],
                "checkpoint_seq": conversation["checkpoint_seq"],
                "requests": [
                    {
                        **{key: row[key] for key in (
                            "request_id", "kind", "mode", "expected_view_revision", "status",
                            "applied_view_revision", "terminal_reason",
                        )},
                        "requested_at": row["requested_at"].isoformat(),
                        "completed_at": (
                            row["completed_at"].isoformat() if row["completed_at"] else None
                        ),
                    }
                    for row in requests
                ],
            }

    async def compact(self, ticket, operation_id, payload):
        if (
            not isinstance(payload, dict)
            or set(payload) != {"expected_view_revision"}
            or type(payload["expected_view_revision"]) is not int
            or payload["expected_view_revision"] < 0
        ):
            raise ForestError(
                "CONTEXT_CONTROL_INVALID", "Compaction requires an expected view revision", 422
            )
        return await self._request(
            ticket, operation_id, payload, action="context.compact", kind="COMPACT"
        )

    async def reset(self, ticket, operation_id, payload):
        if (
            not isinstance(payload, dict)
            or set(payload) != {"expected_view_revision", "mode"}
            or type(payload["expected_view_revision"]) is not int
            or payload["expected_view_revision"] < 0
            or payload["mode"] != "safe-point"
        ):
            raise ForestError(
                "CONTEXT_CONTROL_INVALID",
                "Reset requires an expected view revision and safe-point mode",
                422,
            )
        return await self._request(
            ticket, operation_id, payload, action="context.reset", kind="RESET"
        )

    async def _request(self, ticket, operation_id, payload, *, action, kind):
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, action, operation_id, payload
            )
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            conversation_id = self._conversation_id(claims)
            conversation = await self.messages._conversation(conn, conversation_id)
            if conversation["context_view_revision"] != payload["expected_view_revision"]:
                raise ForestError(
                    "CONTEXT_VIEW_CONFLICT", "Context view revision changed", 409
                )
            request_id = "context-request:" + digest_json([
                self.store.bot_id, conversation_id, operation_id, kind
            ])
            await conn.execute(
                "INSERT INTO public.bot_context_management_requests("
                "bot_id,request_id,operation_id,conversation_id,kind,mode,expected_view_revision) "
                "VALUES (%s,%s,%s,%s,%s,'SAFE_POINT',%s)",
                (
                    self.store.bot_id,
                    request_id,
                    operation_id,
                    conversation_id,
                    kind,
                    payload["expected_view_revision"],
                ),
            )
            await conn.execute(
                "UPDATE public.bot_conversations SET mailbox_revision=mailbox_revision+1,"
                "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s",
                (self.store.bot_id, conversation_id),
            )
            result = {
                "request_id": request_id,
                "conversation_id": conversation_id,
                "kind": kind,
                "mode": "safe-point",
                "status": "PENDING",
                "expected_view_revision": payload["expected_view_revision"],
            }
            await self.control._record(conn, claims, result)
            await self.control._time(conn, claims)
            return result

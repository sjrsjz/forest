"""Versioned per-Binding user policy for route-bound ordinary-message handlers."""

from __future__ import annotations

import json
from uuid import uuid4

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.config_payloads import ConfigPayloadStore
from forest.storage.control import BotControl

MODES = ("DISABLED", "EVERYONE", "ADMINS", "USER_SET")


def normalize_message_handler_policy(values) -> dict:
    try:
        copied = json.loads(json.dumps(values, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError, UnicodeError):
        raise ForestError("MESSAGE_HANDLER_POLICY_INVALID", "Message handler policy must be finite JSON", 422) from None
    if not isinstance(copied, dict) or set(copied) != {"mode", "user_ids"}:
        raise ForestError("MESSAGE_HANDLER_POLICY_INVALID", "Message handler policy fields differ", 422)
    mode, users = copied["mode"], copied["user_ids"]
    if mode not in MODES or not isinstance(users, list) or len(users) > 64:
        raise ForestError("MESSAGE_HANDLER_POLICY_INVALID", "Message handler policy selection is invalid", 422)
    if any(
        not isinstance(user, str) or not user.isascii() or not user.isdecimal()
        or not 1 <= len(user) <= 20 or user.startswith("0")
        for user in users
    ):
        raise ForestError("MESSAGE_HANDLER_POLICY_INVALID", "User IDs must be decimal platform identities", 422)
    users = sorted(set(users), key=lambda value: (len(value), value))
    if (mode == "USER_SET") != bool(users):
        raise ForestError(
            "MESSAGE_HANDLER_POLICY_INVALID",
            "USER_SET requires users and other policy modes must not contain them",
            422,
        )
    return {"mode": mode, "user_ids": users}


class MessageHandlerPolicyStore:
    def __init__(self, store):
        self.store = store
        self.control = BotControl(store)
        self.content = ConfigPayloadStore(store)

    @staticmethod
    def resource_id(binding_id: str, component_id: str, handler_id: str) -> str:
        return digest_json([binding_id, component_id, handler_id])

    async def _binding_in(self, conn, binding_id: str):
        row = await (await conn.execute(
            "SELECT binding_id FROM public.bot_bindings WHERE bot_id=%s AND binding_id=%s FOR SHARE",
            (self.store.bot_id, binding_id),
        )).fetchone()
        if row is None:
            raise ForestError("BINDING_NOT_FOUND", "Binding is unavailable", 404)
        return row

    async def _handler_in(self, conn, component_id: str, handler_id: str):
        await conn.execute(
            "INSERT INTO public.bot_message_handler_clocks(bot_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (self.store.bot_id,),
        )
        clock = await (await conn.execute(
            "SELECT * FROM public.bot_message_handler_clocks WHERE bot_id=%s FOR SHARE",
            (self.store.bot_id,),
        )).fetchone()
        handler = await (await conn.execute(
            "SELECT h.*,g.package_digest FROM public.bot_message_handler_heads h "
            "LEFT JOIN public.bot_message_handler_generations g ON "
            "(g.bot_id,g.generation_id)=(h.bot_id,h.generation_id) "
            "WHERE h.bot_id=%s AND h.component_id=%s AND h.handler_id=%s "
            "AND h.active=true AND h.retired=false",
            (self.store.bot_id, component_id, handler_id),
        )).fetchone()
        if handler is None:
            raise ForestError("MESSAGE_HANDLER_NOT_FOUND", "Message handler is not active", 404)
        return clock, handler

    async def _head_in(self, conn, binding_id: str, component_id: str, handler_id: str):
        resource = self.resource_id(binding_id, component_id, handler_id)
        await conn.execute(
            "INSERT INTO public.bot_message_handler_policy_heads("
            "bot_id,binding_id,component_id,handler_id,resource_id) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (self.store.bot_id, binding_id, component_id, handler_id, resource),
        )
        return await (await conn.execute(
            "SELECT * FROM public.bot_message_handler_policy_heads WHERE bot_id=%s "
            "AND binding_id=%s AND component_id=%s AND handler_id=%s FOR UPDATE",
            (self.store.bot_id, binding_id, component_id, handler_id),
        )).fetchone()

    async def _hydrate_in(self, conn, row, *, kind, required=True):
        if row is None:
            return None
        epoch = (await self.content.payloads.lock_in(conn))["deletion_epoch"]
        body = await self.content.hydrate_values_in(
            conn, row, family="message.handler.policy", kind=kind,
            resource=row["resource_id"],
            identity=row["plan_id"] if kind == "PLAN" else str(row["revision"]),
            epoch=epoch, required=required,
        )
        if body["values_payload_state"] == "ACTIVE":
            values = normalize_message_handler_policy(body["values"])
            if (row["policy_mode"], row["user_count"]) != (values["mode"], len(values["user_ids"])):
                raise ForestError("MESSAGE_HANDLER_POLICY_METADATA_CONFLICT", "Policy metadata differs", 409)
            body = {**body, "values": values}
        return {**row, **body}

    async def read(self, ticket, operation_id: str, binding_id: str) -> dict:
        payload = {"binding_id": binding_id}
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "message.handler.policy.read", operation_id, payload
            )
            await self._binding_in(conn, binding_id)
            await conn.execute(
                "INSERT INTO public.bot_message_handler_clocks(bot_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (self.store.bot_id,),
            )
            clock = await (await conn.execute(
                "SELECT * FROM public.bot_message_handler_clocks WHERE bot_id=%s FOR SHARE",
                (self.store.bot_id,),
            )).fetchone()
            rows = await (await conn.execute(
                "SELECT h.*,g.package_digest,p.revision AS policy_head_revision,r.revision,"
                "r.resource_id,r.plan_id,r.policy_mode,r.user_count,r.values_payload_id,"
                "r.values_payload_digest "
                "FROM public.bot_message_handler_heads h "
                "JOIN public.bot_message_handler_generations g ON "
                "(g.bot_id,g.generation_id)=(h.bot_id,h.generation_id) "
                "LEFT JOIN public.bot_message_handler_policy_heads p ON p.bot_id=h.bot_id "
                "AND p.binding_id=%s AND p.component_id=h.component_id AND p.handler_id=h.handler_id "
                "LEFT JOIN public.bot_message_handler_policy_revisions r ON r.bot_id=p.bot_id "
                "AND r.binding_id=p.binding_id AND r.component_id=p.component_id "
                "AND r.handler_id=p.handler_id AND r.revision=p.revision "
                "WHERE h.bot_id=%s AND h.active=true AND h.retired=false "
                "ORDER BY h.priority DESC,h.component_id,h.handler_id",
                (binding_id, self.store.bot_id),
            )).fetchall()
            handlers = []
            for row in rows:
                revision = row["policy_head_revision"] or 0
                saved = await self._hydrate_in(conn, row, kind="REVISION", required=False) if revision else None
                handlers.append({
                    "handler_id": row["handler_id"], "handler": row["handler"],
                    "component_id": row["component_id"], "package_digest": row["package_digest"],
                    "priority": row["priority"], "spec_digest": row["spec_digest"],
                    "revision": revision,
                    "values": saved["values"] if saved else {"mode": "DISABLED", "user_ids": []},
                    "values_payload_state": saved["values_payload_state"] if saved else "DEFAULT",
                })
            await self.control._time(conn, claims)
            return {
                "binding_id": binding_id, "registry_revision": clock["revision"],
                "modes": list(MODES), "handlers": handlers,
            }

    async def plan(self, ticket, operation_id: str, payload: dict) -> dict:
        if not isinstance(payload, dict) or set(payload) != {
            "binding_id", "component_id", "handler_id", "expected_revision", "values"
        } or type(payload["expected_revision"]) is not int or payload["expected_revision"] < 0:
            raise ForestError("MESSAGE_HANDLER_POLICY_INVALID", "Message handler policy plan fields differ", 422)
        values = normalize_message_handler_policy(payload["values"])
        payload = {**payload, "values": values}
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "message.handler.policy.plan", operation_id, payload
            )
            await self._binding_in(conn, payload["binding_id"])
            _, handler = await self._handler_in(
                conn, payload["component_id"], payload["handler_id"]
            )
            head = await self._head_in(
                conn, payload["binding_id"], payload["component_id"], payload["handler_id"]
            )
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            if head["revision"] != payload["expected_revision"]:
                raise ForestError("REVISION_CONFLICT", "Message handler policy changed; reload it", 409)
            current = None
            if head["revision"]:
                row = await (await conn.execute(
                    "SELECT * FROM public.bot_message_handler_policy_revisions WHERE bot_id=%s "
                    "AND binding_id=%s AND component_id=%s AND handler_id=%s AND revision=%s",
                    (self.store.bot_id, payload["binding_id"], payload["component_id"],
                     payload["handler_id"], head["revision"]),
                )).fetchone()
                current = await self._hydrate_in(conn, row, kind="REVISION", required=False)
            plan_id = str(uuid4())
            plan_digest = digest_json({
                **payload, "bot_id": self.store.bot_id,
                "subject_id": claims["subject_id"], "issuer_id": claims["issuer_id"],
                "handler_spec_digest": handler["spec_digest"],
            })
            original = await self.control.admitted_input_in(conn, claims)
            epoch = (await self.content.payloads.lock_in(conn))["deletion_epoch"]
            ref = await self.content.put_values_in(
                conn, family="message.handler.policy", kind="PLAN", resource=head["resource_id"],
                identity=plan_id, values=values, parents=(original.payload_id,), epoch=epoch,
            )
            await conn.execute(
                "INSERT INTO public.bot_message_handler_policy_plans("
                "bot_id,binding_id,component_id,handler_id,resource_id,plan_id,subject_id,issuer_id,"
                "expected_revision,handler_spec_digest,policy_mode,user_count,plan_digest,expires_at,"
                "values_payload_id,values_payload_digest) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                "clock_timestamp()+interval '10 minutes',%s,%s)",
                (self.store.bot_id, payload["binding_id"], payload["component_id"],
                 payload["handler_id"], head["resource_id"], plan_id, claims["subject_id"],
                 claims["issuer_id"], head["revision"], handler["spec_digest"], values["mode"],
                 len(values["user_ids"]), plan_digest,
                 ref.payload_id, ref.digest),
            )
            result = {
                "binding_id": payload["binding_id"], "component_id": payload["component_id"],
                "handler_id": payload["handler_id"], "handler": handler["handler"],
                "plan_id": plan_id, "plan_digest": plan_digest,
                "expected_revision": head["revision"], "values": values,
                "before_values": (
                    current["values"] if current and current["values_payload_state"] == "ACTIVE"
                    else {"mode": "DISABLED", "user_ids": []} if current is None else None
                ),
                "before_values_payload_state": current["values_payload_state"] if current else "DEFAULT",
            }
            await self.control._record(conn, claims, result, parents=(ref.payload_id,))
            await self.control._time(conn, claims)
            return result

    async def apply(self, ticket, operation_id: str, payload: dict) -> dict:
        if not isinstance(payload, dict) or set(payload) != {
            "binding_id", "component_id", "handler_id", "expected_revision", "plan_id", "plan_digest"
        } or type(payload["expected_revision"]) is not int or payload["expected_revision"] < 0:
            raise ForestError("MESSAGE_HANDLER_POLICY_INVALID", "Message handler policy apply fields differ", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "message.handler.policy.apply", operation_id, payload, payload["plan_digest"]
            )
            await self._binding_in(conn, payload["binding_id"])
            _, handler = await self._handler_in(
                conn, payload["component_id"], payload["handler_id"]
            )
            head = await self._head_in(
                conn, payload["binding_id"], payload["component_id"], payload["handler_id"]
            )
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            plan = await (await conn.execute(
                "SELECT * FROM public.bot_message_handler_policy_plans WHERE bot_id=%s AND plan_id=%s",
                (self.store.bot_id, payload["plan_id"]),
            )).fetchone()
            if plan is None or any(plan[key] != claims[key] for key in ("subject_id", "issuer_id")):
                raise ForestError("PLAN_NOT_FOUND", "Message handler policy plan is unavailable", 404)
            if any(plan[key] != payload[key] for key in (
                "binding_id", "component_id", "handler_id", "expected_revision", "plan_digest"
            )):
                raise ForestError("PLAN_CONFLICT", "Message handler policy plan does not match", 409)
            if head["revision"] != payload["expected_revision"]:
                raise ForestError("REVISION_CONFLICT", "Message handler policy changed after preview", 409)
            if plan["handler_spec_digest"] != handler["spec_digest"]:
                raise ForestError(
                    "MESSAGE_HANDLER_POLICY_PLAN_STALE", "Message handler changed after preview", 409
                )
            if plan["expires_at"] <= await self.store._now(conn):
                raise ForestError("PLAN_EXPIRED", "Preview the message handler policy again", 409)
            plan = await self._hydrate_in(conn, plan, kind="PLAN")
            parent = await self.control.plan_parent(conn, "message.handler.policy", plan)
            original = await self.control.admitted_input_in(conn, claims)
            revision = head["revision"] + 1
            epoch = (await self.content.payloads.lock_in(conn))["deletion_epoch"]
            ref = await self.content.put_values_in(
                conn, family="message.handler.policy", kind="REVISION", resource=head["resource_id"],
                identity=str(revision), values=plan["values"],
                parents=(original.payload_id, parent), epoch=epoch,
            )
            result = {
                "binding_id": payload["binding_id"], "component_id": payload["component_id"],
                "handler_id": payload["handler_id"],
                "revision": revision, "status": "APPLIED",
            }
            await self.control._record(conn, claims, result, parents=(ref.payload_id,))
            await conn.execute(
                "INSERT INTO public.bot_message_handler_policy_revisions("
                "bot_id,binding_id,component_id,handler_id,resource_id,revision,previous_revision,"
                "handler_spec_digest,policy_mode,user_count,policy_revision,plan_id,operation_id,"
                "values_payload_id,values_payload_digest) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (self.store.bot_id, payload["binding_id"], payload["component_id"],
                 payload["handler_id"], head["resource_id"], revision, head["revision"],
                 handler["spec_digest"], plan["values"]["mode"],
                 len(plan["values"]["user_ids"]), claims["policy_revision"], plan["plan_id"],
                 operation_id, ref.payload_id, ref.digest),
            )
            await conn.execute(
                "UPDATE public.bot_message_handler_policy_heads SET revision=%s,updated_at=clock_timestamp() "
                "WHERE bot_id=%s AND binding_id=%s AND component_id=%s AND handler_id=%s",
                (revision, self.store.bot_id, payload["binding_id"], payload["component_id"],
                 payload["handler_id"]),
            )
            await self.control._time(conn, claims)
            if plan["expires_at"] <= await self.store._now(conn):
                raise ForestError("PLAN_EXPIRED", "Message handler policy expired before publication", 409)
            return result

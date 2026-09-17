"""Versioned per-Binding user policy for route-bound business commands."""

from __future__ import annotations

import asyncio
import json
from uuid import uuid4

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.config_payloads import ConfigPayloadStore
from forest.storage.control import BotControl

MODES = ("DISABLED", "EVERYONE", "ADMINS", "USER_SET")


def normalize_policy(values) -> dict:
    try:
        copied = json.loads(json.dumps(values, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError, UnicodeError):
        raise ForestError("COMMAND_POLICY_INVALID", "Command policy must be finite JSON", 422) from None
    if not isinstance(copied, dict) or set(copied) != {"mode", "user_ids"}:
        raise ForestError("COMMAND_POLICY_INVALID", "Command policy fields differ", 422)
    mode, users = copied["mode"], copied["user_ids"]
    if mode not in MODES or not isinstance(users, list) or len(users) > 64:
        raise ForestError("COMMAND_POLICY_INVALID", "Command policy selection is invalid", 422)
    if any(
        not isinstance(user, str) or not user.isascii() or not user.isdecimal()
        or not 1 <= len(user) <= 20 or user.startswith("0")
        for user in users
    ):
        raise ForestError("COMMAND_POLICY_INVALID", "User IDs must be decimal platform identities", 422)
    users = sorted(set(users), key=lambda value: (len(value), value))
    if (mode == "USER_SET") != bool(users):
        raise ForestError(
            "COMMAND_POLICY_INVALID",
            "USER_SET requires users and other policy modes must not contain them",
            422,
        )
    return {"mode": mode, "user_ids": users}


class CommandPolicyStore:
    def __init__(self, store):
        self.store = store
        self.control = BotControl(store)
        self.content = ConfigPayloadStore(store)

    @staticmethod
    def resource_id(binding_id: str, command_id: str) -> str:
        return digest_json([binding_id, command_id])

    async def _binding_in(self, conn, binding_id: str):
        row = await (await conn.execute(
            "SELECT binding_id FROM public.bot_bindings WHERE bot_id=%s AND binding_id=%s FOR SHARE",
            (self.store.bot_id, binding_id),
        )).fetchone()
        if row is None:
            raise ForestError("BINDING_NOT_FOUND", "Binding is unavailable", 404)
        return row

    async def _command_in(self, conn, command_id: str):
        await conn.execute(
            "INSERT INTO public.bot_command_clocks(bot_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (self.store.bot_id,),
        )
        clock = await (await conn.execute(
            "SELECT * FROM public.bot_command_clocks WHERE bot_id=%s FOR SHARE",
            (self.store.bot_id,),
        )).fetchone()
        command = await (await conn.execute(
            "SELECT h.*,g.package_digest FROM public.bot_command_heads h "
            "LEFT JOIN public.bot_command_generations g ON "
            "(g.bot_id,g.generation_id)=(h.bot_id,h.generation_id) "
            "WHERE h.bot_id=%s AND h.command_id=%s AND h.active=true AND h.retired=false",
            (self.store.bot_id, command_id),
        )).fetchone()
        if command is None:
            raise ForestError("COMMAND_NOT_FOUND", "Business command is not active", 404)
        return clock, command

    async def _head_in(self, conn, binding_id: str, command_id: str):
        resource = self.resource_id(binding_id, command_id)
        await conn.execute(
            "INSERT INTO public.bot_command_policy_heads(bot_id,binding_id,command_id,resource_id) "
            "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (self.store.bot_id, binding_id, command_id, resource),
        )
        return await (await conn.execute(
            "SELECT * FROM public.bot_command_policy_heads WHERE bot_id=%s "
            "AND binding_id=%s AND command_id=%s FOR UPDATE",
            (self.store.bot_id, binding_id, command_id),
        )).fetchone()

    async def _hydrate_in(self, conn, row, *, kind, required=True):
        if row is None:
            return None
        epoch = (await self.content.payloads.lock_in(conn))["deletion_epoch"]
        body = await self.content.hydrate_values_in(
            conn, row, family="command.policy", kind=kind,
            resource=row["resource_id"],
            identity=row["plan_id"] if kind == "PLAN" else str(row["revision"]),
            epoch=epoch, required=required,
        )
        if body["values_payload_state"] == "ACTIVE":
            values = normalize_policy(body["values"])
            if (row["policy_mode"], row["user_count"]) != (values["mode"], len(values["user_ids"])):
                raise ForestError("COMMAND_POLICY_METADATA_CONFLICT", "Policy metadata differs", 409)
            body = {**body, "values": values}
        return {**row, **body}

    async def read(self, ticket, operation_id: str, binding_id: str) -> dict:
        payload = {"binding_id": binding_id}
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "command.policy.read", operation_id, payload
            )
            await self._binding_in(conn, binding_id)
            await conn.execute(
                "INSERT INTO public.bot_command_clocks(bot_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (self.store.bot_id,),
            )
            clock = await (await conn.execute(
                "SELECT * FROM public.bot_command_clocks WHERE bot_id=%s FOR SHARE",
                (self.store.bot_id,),
            )).fetchone()
            rows = await (await conn.execute(
                "SELECT h.*,g.package_digest,p.revision AS policy_head_revision,r.revision,"
                "r.resource_id,r.plan_id,r.policy_mode,r.user_count,r.values_payload_id,"
                "r.values_payload_digest "
                "FROM public.bot_command_heads h "
                "JOIN public.bot_command_generations g ON "
                "(g.bot_id,g.generation_id)=(h.bot_id,h.generation_id) "
                "LEFT JOIN public.bot_command_policy_heads p ON p.bot_id=h.bot_id "
                "AND p.binding_id=%s AND p.command_id=h.command_id "
                "LEFT JOIN public.bot_command_policy_revisions r ON r.bot_id=p.bot_id "
                "AND r.binding_id=p.binding_id AND r.command_id=p.command_id AND r.revision=p.revision "
                "WHERE h.bot_id=%s AND h.active=true AND h.retired=false ORDER BY h.name,h.command_id",
                (binding_id, self.store.bot_id),
            )).fetchall()
            commands = []
            for row in rows:
                revision = row["policy_head_revision"] or 0
                saved = await self._hydrate_in(conn, row, kind="REVISION", required=False) if revision else None
                commands.append({
                    "command_id": row["command_id"], "name": row["name"],
                    "component_id": row["component_id"], "package_digest": row["package_digest"],
                    "permission": row["permission"], "argument_mode": row["argument_mode"],
                    "priority": row["priority"], "spec_digest": row["spec_digest"],
                    "revision": revision,
                    "values": saved["values"] if saved else {"mode": "DISABLED", "user_ids": []},
                    "values_payload_state": saved["values_payload_state"] if saved else "DEFAULT",
                })
            await self.control._time(conn, claims)
            return {
                "binding_id": binding_id, "registry_revision": clock["revision"],
                "modes": list(MODES), "commands": commands,
            }

    async def plan(self, ticket, operation_id: str, payload: dict) -> dict:
        if not isinstance(payload, dict) or set(payload) != {
            "binding_id", "command_id", "expected_revision", "values"
        } or type(payload["expected_revision"]) is not int or payload["expected_revision"] < 0:
            raise ForestError("COMMAND_POLICY_INVALID", "Command policy plan fields differ", 422)
        values = normalize_policy(payload["values"])
        payload = {**payload, "values": values}
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "command.policy.plan", operation_id, payload
            )
            await self._binding_in(conn, payload["binding_id"])
            _, command = await self._command_in(conn, payload["command_id"])
            head = await self._head_in(conn, payload["binding_id"], payload["command_id"])
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            if head["revision"] != payload["expected_revision"]:
                raise ForestError("REVISION_CONFLICT", "Command policy changed; reload it", 409)
            current = None
            if head["revision"]:
                row = await (await conn.execute(
                    "SELECT * FROM public.bot_command_policy_revisions WHERE bot_id=%s "
                    "AND binding_id=%s AND command_id=%s AND revision=%s",
                    (self.store.bot_id, payload["binding_id"], payload["command_id"], head["revision"]),
                )).fetchone()
                current = await self._hydrate_in(conn, row, kind="REVISION", required=False)
            plan_id = str(uuid4())
            plan_digest = digest_json({
                **payload, "bot_id": self.store.bot_id,
                "subject_id": claims["subject_id"], "issuer_id": claims["issuer_id"],
                "command_spec_digest": command["spec_digest"],
            })
            original = await self.control.admitted_input_in(conn, claims)
            epoch = (await self.content.payloads.lock_in(conn))["deletion_epoch"]
            ref = await self.content.put_values_in(
                conn, family="command.policy", kind="PLAN", resource=head["resource_id"],
                identity=plan_id, values=values, parents=(original.payload_id,), epoch=epoch,
            )
            await conn.execute(
                "INSERT INTO public.bot_command_policy_plans("
                "bot_id,binding_id,command_id,resource_id,plan_id,subject_id,issuer_id,"
                "expected_revision,command_spec_digest,policy_mode,user_count,plan_digest,expires_at,"
                "values_payload_id,values_payload_digest) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp()+interval '10 minutes',%s,%s)",
                (self.store.bot_id, payload["binding_id"], payload["command_id"], head["resource_id"],
                 plan_id, claims["subject_id"], claims["issuer_id"], head["revision"],
                 command["spec_digest"], values["mode"], len(values["user_ids"]), plan_digest,
                 ref.payload_id, ref.digest),
            )
            result = {
                "binding_id": payload["binding_id"], "command_id": payload["command_id"],
                "name": command["name"], "plan_id": plan_id, "plan_digest": plan_digest,
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
        try:
            return await self._apply(ticket, operation_id, payload)
        except ForestError as exc:
            if exc.code != "CONTROL_CONTENT_REVOKED":
                raise
            # A concurrent winner can advance the policy head while the losing
            # transaction is resolving its immutable plan payload. Re-read the
            # head after rollback so the public conflict is determined by the
            # authoritative revision rather than payload cleanup timing.
            # The revocation can be observed before the concurrent publisher
            # reaches the policy-head lock. Wait for that bounded publication
            # boundary instead of exposing a cleanup race as the public result.
            for delay in (0, 0.005, 0.01, 0.02, 0.04, 0.08, 0.16):
                if delay:
                    await asyncio.sleep(delay)
                async with self.store.pool.connection() as conn:
                    row = await (
                        await conn.execute(
                            "SELECT revision FROM public.bot_command_policy_heads "
                            "WHERE bot_id=%s AND binding_id=%s AND command_id=%s FOR SHARE",
                            (
                                self.store.bot_id,
                                payload.get("binding_id"),
                                payload.get("command_id"),
                            ),
                        )
                    ).fetchone()
                if row is not None and row["revision"] != payload.get("expected_revision"):
                    raise ForestError(
                        "REVISION_CONFLICT", "Command policy changed after preview", 409
                    ) from exc
            raise

    async def _apply(self, ticket, operation_id: str, payload: dict) -> dict:
        if not isinstance(payload, dict) or set(payload) != {
            "binding_id", "command_id", "expected_revision", "plan_id", "plan_digest"
        } or type(payload["expected_revision"]) is not int or payload["expected_revision"] < 0:
            raise ForestError("COMMAND_POLICY_INVALID", "Command policy apply fields differ", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "command.policy.apply", operation_id, payload, payload["plan_digest"]
            )
            await self._binding_in(conn, payload["binding_id"])
            _, command = await self._command_in(conn, payload["command_id"])
            head = await self._head_in(conn, payload["binding_id"], payload["command_id"])
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            plan = await (await conn.execute(
                "SELECT * FROM public.bot_command_policy_plans WHERE bot_id=%s AND plan_id=%s",
                (self.store.bot_id, payload["plan_id"]),
            )).fetchone()
            if plan is None or any(plan[key] != claims[key] for key in ("subject_id", "issuer_id")):
                raise ForestError("PLAN_NOT_FOUND", "Command policy plan is unavailable", 404)
            if any(plan[key] != payload[key] for key in (
                "binding_id", "command_id", "expected_revision", "plan_digest"
            )):
                raise ForestError("PLAN_CONFLICT", "Command policy plan does not match", 409)
            if head["revision"] != payload["expected_revision"]:
                raise ForestError("REVISION_CONFLICT", "Command policy changed after preview", 409)
            if plan["command_spec_digest"] != command["spec_digest"]:
                raise ForestError("COMMAND_POLICY_PLAN_STALE", "Command changed after preview", 409)
            if plan["expires_at"] <= await self.store._now(conn):
                raise ForestError("PLAN_EXPIRED", "Preview the command policy again", 409)
            plan = await self._hydrate_in(conn, plan, kind="PLAN")
            parent = await self.control.plan_parent(conn, "command.policy", plan)
            original = await self.control.admitted_input_in(conn, claims)
            revision = head["revision"] + 1
            epoch = (await self.content.payloads.lock_in(conn))["deletion_epoch"]
            ref = await self.content.put_values_in(
                conn, family="command.policy", kind="REVISION", resource=head["resource_id"],
                identity=str(revision), values=plan["values"],
                parents=(original.payload_id, parent), epoch=epoch,
            )
            result = {
                "binding_id": payload["binding_id"], "command_id": payload["command_id"],
                "revision": revision, "status": "APPLIED",
            }
            await self.control._record(conn, claims, result, parents=(ref.payload_id,))
            await conn.execute(
                "INSERT INTO public.bot_command_policy_revisions("
                "bot_id,binding_id,command_id,resource_id,revision,previous_revision,"
                "command_spec_digest,policy_mode,user_count,policy_revision,plan_id,operation_id,"
                "values_payload_id,values_payload_digest) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (self.store.bot_id, payload["binding_id"], payload["command_id"], head["resource_id"],
                 revision, head["revision"], command["spec_digest"], plan["values"]["mode"],
                 len(plan["values"]["user_ids"]), claims["policy_revision"], plan["plan_id"],
                 operation_id, ref.payload_id, ref.digest),
            )
            await conn.execute(
                "UPDATE public.bot_command_policy_heads SET revision=%s,updated_at=clock_timestamp() "
                "WHERE bot_id=%s AND binding_id=%s AND command_id=%s",
                (revision, self.store.bot_id, payload["binding_id"], payload["command_id"]),
            )
            allowed_users = None
            if plan["values"]["mode"] == "DISABLED":
                allowed_users = []
            elif plan["values"]["mode"] == "USER_SET":
                allowed_users = plan["values"]["user_ids"]
            elif plan["values"]["mode"] == "ADMINS":
                from forest.storage.bindings import BindingStore

                current = await BindingStore.current_config_in(
                    conn,
                    self.store.bot_id,
                    payload["binding_id"],
                    required=True,
                )
                allowed_users = current["config"]["admin_user_ids"]
            if allowed_users is not None:
                await conn.execute(
                    "UPDATE public.bot_business_command_requests SET status='CANCELLED',"
                    "terminal_reason='COMMAND_POLICY_REVOKED',completed_at=clock_timestamp() "
                    "WHERE bot_id=%s AND binding_id=%s AND command_id=%s AND status='PENDING' "
                    "AND NOT (external_user_id=ANY(%s))",
                    (
                        self.store.bot_id,
                        payload["binding_id"],
                        payload["command_id"],
                        allowed_users,
                    ),
                )
            await self.control._time(conn, claims)
            if plan["expires_at"] <= await self.store._now(conn):
                raise ForestError("PLAN_EXPIRED", "Command policy expired before publication", 409)
            return result

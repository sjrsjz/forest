"""Target-Bot Agent configuration, immutable revisions and separately encrypted keys."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from psycopg.types.json import Jsonb

from forest.agent_protocol import protocol_descriptor
from forest.agent_settings import (
    DEFAULT_AGENT_SETTINGS,
    AgentSettings,
    normalize_stored_agent_values,
)
from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.agent_specs import values_metadata
from forest.storage.bindings import _clone, _revision
from forest.storage.config_payloads import ConfigPayloadStore
from forest.storage.control import BotControl


class AgentConfig:
    def __init__(self, store, vault=None):
        self.store, self.vault = store, vault
        self.control = BotControl(store)
        self.payloads = ConfigPayloadStore(store)

    async def _head(self, conn):
        await conn.execute("INSERT INTO public.bot_agent_config_heads(bot_id) VALUES (%s) ON CONFLICT DO NOTHING", (self.store.bot_id,))
        return await (await conn.execute("SELECT * FROM public.bot_agent_config_heads WHERE bot_id=%s FOR UPDATE", (self.store.bot_id,))).fetchone()

    async def _hydrate(self, conn, row, *, kind, required=True):
        if row is None:
            return None
        epoch = (await self.payloads.payloads.lock_in(conn))["deletion_epoch"]
        body = await self.payloads.hydrate_values_in(
            conn, row, family="agent.config", kind=kind, resource="",
            identity=row["plan_id"] if kind == "PLAN" else str(row["revision"]),
            epoch=epoch, required=required,
        )
        stored_values = body["values"]
        if stored_values is None:
            values = None
        else:
            try:
                values = normalize_stored_agent_values(stored_values)
            except ValueError:
                raise ForestError(
                    "AGENT_CONFIG_INVALID",
                    "Stored Agent settings do not match the current schema",
                    409,
                ) from None
        return {**row, **body, "stored_values": stored_values, "values": values}

    async def _revision(self, conn, revision, *, required=True):
        row = await (await conn.execute("SELECT * FROM public.bot_agent_config_revisions WHERE bot_id=%s AND revision=%s", (self.store.bot_id, revision))).fetchone()
        return await self._hydrate(conn, row, kind="REVISION", required=required)

    async def read(self, ticket, operation_id):
        ticket = _clone(ticket)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "agent.config.read", operation_id, {})
            head = await self._head(conn)
            saved = await self._revision(conn, head["revision"], required=False)
            await self.control._time(conn, claims)
            return {"revision": head["revision"], "values": saved["values"] if saved else DEFAULT_AGENT_SETTINGS,
                    "defaults": _clone(DEFAULT_AGENT_SETTINGS),
                    "protocol": protocol_descriptor(),
                    "values_payload_state": saved["values_payload_state"] if saved else "ABSENT",
                    "has_api_key": bool(saved and self.vault.decrypt(saved["secret_ciphertext"])),
                    "secret_revision": head["secret_revision"]}

    async def plan(self, ticket, operation_id, payload, *, secret_ciphertext=None):
        ticket, payload = _clone(ticket), _clone(payload)
        if set(payload) != {"expected_revision", "values", "credentials_changed", "secret_digest"}:
            raise ForestError("AGENT_CONFIG_INVALID", "Agent plan fields differ", 422)
        _revision(payload["expected_revision"])
        try:
            values = AgentSettings.model_validate(payload["values"]).model_dump()
        except ValueError:
            raise ForestError("AGENT_CONFIG_INVALID", "Agent settings do not match the schema", 422) from None
        if values != payload["values"] or type(payload["credentials_changed"]) is not bool:
            raise ForestError("AGENT_CONFIG_INVALID", "Agent plan must contain normalized settings", 422)
        if payload["credentials_changed"]:
            if not secret_ciphertext or digest_json({"api_key": self.vault.decrypt(secret_ciphertext)}) != payload["secret_digest"]:
                raise ForestError("CREDENTIALS_CONFLICT", "API key differs from the signed request")
        elif payload["secret_digest"] != digest_json({"api_key": None}):
            raise ForestError("CREDENTIALS_CONFLICT", "An unchanged key has no replacement input")
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "agent.config.plan", operation_id, payload)
            head = await self._head(conn)
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            if head["revision"] != payload["expected_revision"]:
                raise ForestError("REVISION_CONFLICT", "Agent settings changed; reload before applying")
            saved = await self._revision(conn, head["revision"], required=False)
            cipher = secret_ciphertext if payload["credentials_changed"] else (saved["secret_ciphertext"] if saved else self.vault.encrypt(""))
            plan_id = str(uuid4())
            digest = digest_json({**payload, "bot_id": self.store.bot_id, "subject_id": claims["subject_id"], "issuer_id": claims["issuer_id"]})
            original = await self.control.admitted_input_in(conn, claims)
            epoch = (await self.payloads.payloads.lock_in(conn))["deletion_epoch"]
            value_ref = await self.payloads.put_values_in(
                conn, family="agent.config", kind="PLAN", resource="", identity=plan_id,
                values=values, parents=(original.payload_id,), epoch=epoch,
            )
            await conn.execute(
                "INSERT INTO public.bot_agent_config_plans(bot_id,plan_id,subject_id,issuer_id,expected_revision,values,secret_ciphertext,credentials_changed,plan_digest,expires_at,values_payload_id,values_payload_digest,values_metadata) "
                "VALUES (%s,%s,%s,%s,%s,NULL,%s,%s,%s,clock_timestamp()+interval '10 minutes',%s,%s,%s)",
                (self.store.bot_id, plan_id, claims["subject_id"], claims["issuer_id"], head["revision"], cipher, payload["credentials_changed"], digest,
                 value_ref.payload_id, value_ref.digest, Jsonb(values_metadata(values))),
            )
            result = {"plan_id": plan_id, "plan_digest": digest, "expected_revision": head["revision"],
                      "values": values, "before_values": saved["values"] if saved else DEFAULT_AGENT_SETTINGS,
                      "before_values_payload_state": saved["values_payload_state"] if saved else "ABSENT",
                      "credentials_changed": payload["credentials_changed"]}
            parents = (value_ref.payload_id,)
            if saved and saved["values_payload_state"] == "ACTIVE":
                parents += (saved["values_payload_id"],)
            await self.control._record(conn, claims, result, parents=parents)
            await self.control._time(conn, claims)
            return result

    async def apply(self, ticket, operation_id, payload):
        ticket, payload = _clone(ticket), _clone(payload)
        if set(payload) != {"expected_revision", "plan_id", "plan_digest"}:
            raise ForestError("AGENT_CONFIG_INVALID", "Agent apply fields differ", 422)
        _revision(payload["expected_revision"])
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "agent.config.apply", operation_id, payload, payload["plan_digest"])
            head = await self._head(conn)
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            plan = await (await conn.execute("SELECT * FROM public.bot_agent_config_plans WHERE bot_id=%s AND plan_id=%s", (self.store.bot_id, payload["plan_id"]))).fetchone()
            if not plan or any(plan[key] != claims[key] for key in ("subject_id", "issuer_id")):
                raise ForestError("PLAN_NOT_FOUND", "Agent plan is unavailable", 404)
            if any(plan[key] != payload[key] for key in ("expected_revision", "plan_digest")):
                raise ForestError("PLAN_CONFLICT", "Agent plan does not match")
            if head["revision"] != payload["expected_revision"]:
                raise ForestError("REVISION_CONFLICT", "Agent settings were changed in another request")
            if plan["expires_at"] <= await self.store._now(conn):
                raise ForestError("PLAN_EXPIRED", "Preview the Agent settings again")
            plan = await self._hydrate(conn, plan, kind="PLAN")
            parent = await self.control.plan_parent(conn, "agent.config", plan)
            revision = head["revision"] + 1
            secret_revision = head["secret_revision"] + int(plan["credentials_changed"] or head["revision"] == 0)
            result = {"revision": revision, "status": "APPLIED", "secret_revision": secret_revision}
            original = await self.control.admitted_input_in(conn, claims)
            epoch = (await self.payloads.payloads.lock_in(conn))["deletion_epoch"]
            value_ref = await self.payloads.put_values_in(
                conn, family="agent.config", kind="REVISION", resource="", identity=str(revision),
                values=plan["values"], parents=(original.payload_id, parent), epoch=epoch,
            )
            await self.control._record(conn, claims, result, parents=(value_ref.payload_id,))
            await conn.execute(
                "INSERT INTO public.bot_agent_config_revisions(bot_id,revision,previous_revision,values,secret_revision,secret_ciphertext,policy_revision,plan_id,operation_id,values_payload_id,values_payload_digest,values_metadata) VALUES (%s,%s,%s,NULL,%s,%s,%s,%s,%s,%s,%s,%s)",
                (self.store.bot_id, revision, head["revision"], secret_revision, plan["secret_ciphertext"], claims["policy_revision"], plan["plan_id"], operation_id,
                 value_ref.payload_id, value_ref.digest, Jsonb(values_metadata(plan["values"]))),
            )
            await conn.execute("UPDATE public.bot_agent_config_heads SET revision=%s,secret_revision=%s WHERE bot_id=%s", (revision, secret_revision, self.store.bot_id))
            # A waiting Run must be considered again after disable/revoke/config changes.
            await conn.execute("UPDATE public.bot_conversations SET mailbox_revision=mailbox_revision+1 WHERE bot_id=%s AND active_session_id IS NOT NULL", (self.store.bot_id,))
            await self.control._time(conn, claims)
            if plan["expires_at"] <= await self.store._now(conn):
                raise ForestError("PLAN_EXPIRED", "Agent plan expired before publication")
            return result

    @staticmethod
    async def guard_in(conn, actor, config_revision):
        """Called AFTER the Actor lock chain inside the caller's final transaction."""
        rows = await (await conn.execute(
            "SELECT to_jsonb(r) AS frozen,to_jsonb(current) AS current "
            "FROM public.bot_agent_config_heads h JOIN public.bot_agent_config_revisions current ON "
            "(current.bot_id,current.revision)=(h.bot_id,h.revision) JOIN public.bot_agent_config_revisions r "
            "ON r.bot_id=h.bot_id AND r.revision=%s WHERE h.bot_id=%s FOR SHARE OF h",
            (config_revision, actor.host.bot_id),
        )).fetchone()
        if not rows:
            raise ForestError("AGENT_CONFIGURATION_REVOKED", "Agent or its frozen credentials are no longer authorized", 403)
        reader = AgentConfig(SimpleNamespace(bot_id=actor.host.bot_id))
        try:
            frozen = await reader._hydrate(conn, rows["frozen"], kind="REVISION")
            current = await reader._hydrate(conn, rows["current"], kind="REVISION")
        except ForestError as exc:
            if exc.code == "CONTROL_CONTENT_REVOKED":
                raise ForestError("AGENT_CONFIGURATION_REVOKED", "Agent configuration content is unavailable", 403) from None
            raise
        if (not current["values"]["enabled"] or not frozen["values"]["enabled"]
                or frozen["policy_revision"] != actor.policy_revision
                or frozen["secret_revision"] != current["secret_revision"]):
            raise ForestError("AGENT_CONFIGURATION_REVOKED", "Agent or its frozen credentials are no longer authorized", 403)
        return {"values": frozen["values"], "stored_values": frozen["stored_values"],
                "current_values": current["values"],
                "values_payload_id": frozen["values_payload_id"], "current_values_payload_id": current["values_payload_id"],
                "daily_token_budget": min(frozen["values"]["daily_token_budget"], current["values"]["daily_token_budget"]),
                "secret_revision": frozen["secret_revision"]}

    async def runtime(self, lease, *, revision=None):
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            head = await self._head(conn)
            saved = await self._revision(conn, head["revision"] if revision is None else revision)
            if not saved:
                raise ForestError("AGENT_NOT_CONFIGURED", "Configure this Bot's model before enabling Agent replies", 409)
            await self.store._host(conn, lease)
            return saved

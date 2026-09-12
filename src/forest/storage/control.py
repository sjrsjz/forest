"""Target-Bot management acceptance; deployment sessions are never a Bot repository."""

from __future__ import annotations

import base64
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.bot import BotStore, HostLease

ACTIONS = ("config.read", "config.plan", "config.apply", "service.invoke",
           "binding.create", "binding.read", "binding.plan", "binding.apply",
           "message.read", "message.cancel", "delivery.reconcile")
_REVISION_CLAIMS = frozenset(
    {"key_revision", "account_revision", "session_revision", "policy_revision"}
)
_TEXT_CLAIMS = frozenset(
    {
        "ticket_id",
        "issuer_id",
        "subject_id",
        "session_ref",
        "bot_id",
        "operation_id",
        "action",
        "input_digest",
        "plan_digest",
        "issued_at",
        "expires_at",
    }
)
_CLAIMS = _REVISION_CLAIMS | _TEXT_CLAIMS


class BotControl:
    def __init__(self, store: BotStore):
        self.store = store

    async def trust(
        self, lease: HostLease, *, issuer_id: str, key_revision: int, public_key: str
    ) -> None:
        """Host-only initial trust bootstrap for a directory-owned Bot database."""
        Ed25519PublicKey.from_public_bytes(base64.b64decode(public_key, validate=True))
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_web_issuers WHERE bot_id=%s FOR UPDATE",
                    (self.store.bot_id,),
                )
            ).fetchone()
            await self.store._host(conn, lease)
            if row is not None:
                if (row["issuer_id"], row["key_revision"], row["public_key"]) != (
                    issuer_id,
                    key_revision,
                    public_key,
                ):
                    raise ForestError("AUTHORITY_KEY_MISMATCH", "Bot trusts another authority", 403)
                return
            await conn.execute(
                "INSERT INTO public.bot_web_issuers(bot_id,issuer_id,key_revision,public_key,actions) "
                "VALUES (%s,%s,%s,%s,%s)",
                (self.store.bot_id, issuer_id, key_revision, public_key, Jsonb(list(ACTIONS))),
            )
            await self.store._host(conn, lease)

    async def _guard(
        self,
        conn,
        ticket: dict,
        action: str,
        operation_id: str,
        payload: dict,
        plan_digest: str = "",
    ) -> dict:
        # Runtime lock serializes policy revocation with target acceptance.
        runtime = await self.store._runtime(conn)
        try:
            if not isinstance(ticket, dict) or set(ticket) != {"claims", "signature"}:
                raise ValueError("ticket shape")
            claims = ticket["claims"]
            if not isinstance(claims, dict) or set(claims) != _CLAIMS:
                raise ValueError("claims shape")
            if any(type(claims[key]) is not int or claims[key] <= 0 for key in _REVISION_CLAIMS):
                raise ValueError("revision")
            if any(
                not isinstance(claims[key], str)
                or len(claims[key]) > 4096
                or (not claims[key] and key != "plan_digest")
                for key in _TEXT_CLAIMS
            ):
                raise ValueError("text claims")
            if not isinstance(ticket["signature"], str) or len(ticket["signature"]) > 256:
                raise ValueError("signature")
            UUID(claims["ticket_id"])
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_web_issuers WHERE bot_id=%s AND issuer_id=%s FOR SHARE",
                    (self.store.bot_id, claims["issuer_id"]),
                )
            ).fetchone()
            if row is None or not row["enabled"] or action not in row["actions"]:
                raise ValueError("issuer")
            Ed25519PublicKey.from_public_bytes(
                base64.b64decode(row["public_key"], validate=True)
            ).verify(
                base64.b64decode(ticket["signature"], validate=True),
                digest_json(claims).encode("ascii"),
            )
            if any(
                (
                    claims["bot_id"] != self.store.bot_id,
                    claims["action"] != action,
                    claims["operation_id"] != operation_id,
                    claims["input_digest"] != digest_json(payload),
                    claims["plan_digest"] != plan_digest,
                    claims["key_revision"] != row["key_revision"],
                    claims["policy_revision"] != runtime["policy_revision"],
                )
            ):
                raise ValueError("scope")
            issued, expiry = (
                datetime.fromisoformat(claims["issued_at"]),
                datetime.fromisoformat(claims["expires_at"]),
            )
            if issued.utcoffset() is None or expiry.utcoffset() is None:
                raise ValueError("timestamp offset")
            now = await self.store._now(conn)
            if not issued <= now < expiry or not timedelta(0) < expiry - issued <= timedelta(
                seconds=30
            ):
                raise ValueError("expiry")
        except (KeyError, TypeError, ValueError, InvalidSignature):
            raise ForestError(
                "WEB_AUTHORITY_REJECTED", "Target Web authority is invalid or expired", 403
            ) from None
        if runtime["status"] != "ACTIVE" or not runtime["control_admission"]:
            raise ForestError("CONTROL_CLOSED", "Bot control admission is closed", 409)
        return claims

    async def _time(self, conn, claims: dict) -> None:
        # Called again after business locks; a ticket may expire while waiting.
        if await self.store._now(conn) >= datetime.fromisoformat(claims["expires_at"]):
            raise ForestError("WEB_AUTHORITY_REJECTED", "Target Web authority expired", 403)

    async def _head(self, conn, component_id: str) -> dict:
        await conn.execute(
            "INSERT INTO public.bot_config_heads(bot_id,component_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (self.store.bot_id, component_id),
        )
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_config_heads WHERE bot_id=%s AND component_id=%s FOR UPDATE",
                (self.store.bot_id, component_id),
            )
        ).fetchone()

    async def _replay(self, conn, claims: dict) -> dict | None:
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_web_operations WHERE bot_id=%s AND operation_id=%s",
                (self.store.bot_id, claims["operation_id"]),
            )
        ).fetchone()
        if row is not None:
            if any(
                row[k] != claims[k] for k in ("action", "input_digest", "subject_id", "issuer_id")
            ):
                raise ForestError(
                    "IDEMPOTENCY_CONFLICT", "Management operation has different input", 409
                )
            return row["result"]
        return None

    async def _record(self, conn, claims: dict, result: dict) -> None:
        await conn.execute(
            "INSERT INTO public.bot_web_operations(bot_id,operation_id,action,input_digest,subject_id,"
            "issuer_id,ticket_basis,result) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                self.store.bot_id,
                claims["operation_id"],
                claims["action"],
                claims["input_digest"],
                claims["subject_id"],
                claims["issuer_id"],
                Jsonb(claims),
                Jsonb(result),
            ),
        )
        await self.store._event(
            conn,
            claims["action"],
            claims["operation_id"],
            {"subject_id": claims["subject_id"], **result},
        )

    async def read(self, ticket: dict, operation_id: str, component_id: str) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self._guard(
                conn, ticket, "config.read", operation_id, {"component_id": component_id}
            )
            # One statement gives matching desired/effective revisions and values.
            row = await (
                await conn.execute(
                    "SELECT h.desired_revision,h.effective_revision,d.values AS desired_values,"
                    "e.values AS effective_values FROM public.bot_config_heads h "
                    "LEFT JOIN public.bot_config_revisions d ON (d.bot_id,d.component_id,d.revision)="
                    "(h.bot_id,h.component_id,h.desired_revision) "
                    "LEFT JOIN public.bot_config_revisions e ON (e.bot_id,e.component_id,e.revision)="
                    "(h.bot_id,h.component_id,h.effective_revision) WHERE h.bot_id=%s AND h.component_id=%s",
                    (self.store.bot_id, component_id),
                )
            ).fetchone()
            await self._time(conn, claims)
            return row or {
                "desired_revision": 0,
                "effective_revision": 0,
                "desired_values": None,
                "effective_values": None,
            }

    async def plan(self, ticket: dict, operation_id: str, payload: dict) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self._guard(conn, ticket, "config.plan", operation_id, payload)
            head = await self._head(conn, payload["component_id"])
            await self._time(conn, claims)
            replay = await self._replay(conn, claims)
            if replay is not None:
                return replay
            if head["desired_revision"] != payload["expected_revision"]:
                raise ForestError(
                    "REVISION_CONFLICT", "Bot configuration changed; reload before applying", 409
                )
            plan_id = str(uuid4())
            digest = digest_json(
                {
                    **payload,
                    "bot_id": self.store.bot_id,
                    "subject_id": claims["subject_id"],
                    "issuer_id": claims["issuer_id"],
                }
            )
            await conn.execute(
                "INSERT INTO public.bot_config_plans(bot_id,plan_id,component_id,subject_id,issuer_id,"
                "expected_revision,values,plan_digest,expires_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp()+interval '10 minutes')",
                (
                    self.store.bot_id,
                    plan_id,
                    payload["component_id"],
                    claims["subject_id"],
                    claims["issuer_id"],
                    payload["expected_revision"],
                    Jsonb(payload["values"]),
                    digest,
                ),
            )
            result = {
                "plan_id": plan_id,
                "plan_digest": digest,
                "expected_revision": payload["expected_revision"],
                "values": payload["values"],
            }
            await self._record(conn, claims, result)
            return result

    async def apply(self, ticket: dict, operation_id: str, payload: dict) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self._guard(
                conn, ticket, "config.apply", operation_id, payload, payload["plan_digest"]
            )
            head = await self._head(conn, payload["component_id"])
            await self._time(conn, claims)
            replay = await self._replay(conn, claims)
            if replay is not None:
                return replay
            saved = await (
                await conn.execute(
                    "SELECT * FROM public.bot_config_plans WHERE bot_id=%s AND plan_id=%s",
                    (self.store.bot_id, payload["plan_id"]),
                )
            ).fetchone()
            if saved is None or any(saved[k] != claims[k] for k in ("subject_id", "issuer_id")):
                raise ForestError("PLAN_NOT_FOUND", "Bot configuration plan is unavailable", 404)
            if (
                saved["component_id"] != payload["component_id"]
                or saved["plan_digest"] != payload["plan_digest"]
                or saved["expected_revision"] != payload["expected_revision"]
            ):
                raise ForestError("PLAN_CONFLICT", "Configuration plan does not match", 409)
            if saved["expires_at"] <= await self.store._now(conn):
                raise ForestError("PLAN_EXPIRED", "Preview the configuration again", 409)
            if head["desired_revision"] != payload["expected_revision"]:
                raise ForestError(
                    "REVISION_CONFLICT", "Bot configuration was changed in another request", 409
                )
            revision = head["desired_revision"] + 1
            result = {
                "operation_id": operation_id,
                "desired_revision": revision,
                "effective_revision": head["effective_revision"],
                "status": "ACCEPTED",
            }
            await self._record(conn, claims, result)
            await conn.execute(
                "INSERT INTO public.bot_config_revisions(bot_id,component_id,revision,previous_revision,"
                "values,policy_revision,plan_id,operation_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    payload["component_id"],
                    revision,
                    revision - 1,
                    Jsonb(saved["values"]),
                    claims["policy_revision"],
                    saved["plan_id"],
                    operation_id,
                ),
            )
            await conn.execute(
                "UPDATE public.bot_config_heads SET desired_revision=%s WHERE bot_id=%s AND component_id=%s",
                (revision, self.store.bot_id, payload["component_id"]),
            )
            return result

    async def desired(self, lease: HostLease) -> list[dict]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            return await (
                await conn.execute(
                    "SELECT r.*,h.effective_revision FROM public.bot_config_heads h "
                    "JOIN public.bot_config_revisions r ON (r.bot_id,r.component_id,r.revision)="
                    "(h.bot_id,h.component_id,h.desired_revision) WHERE h.bot_id=%s ORDER BY h.component_id",
                    (self.store.bot_id,),
                )
            ).fetchall()

    async def bind_activation(
        self, lease: HostLease, component_id: str, revision: int, operation_id: str
    ) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            head = await self._head(conn, component_id)
            await self.store._host(conn, lease, runtime)
            saved = await (
                await conn.execute(
                    "SELECT * FROM public.bot_config_revisions WHERE bot_id=%s AND component_id=%s AND revision=%s",
                    (self.store.bot_id, component_id, revision),
                )
            ).fetchone()
            if saved is None or head["desired_revision"] != revision:
                raise ForestError(
                    "CONFIGURATION_SUPERSEDED", "A newer configuration is pending", 409
                )
            if (
                saved["policy_revision"] != runtime["policy_revision"]
                or not runtime["control_admission"]
            ):
                raise ForestError(
                    "POLICY_REVOKED",
                    "Configuration must be authorized under current Bot policy",
                    403,
                )
            await conn.execute(
                "INSERT INTO public.bot_config_activations(bot_id,operation_id,component_id,revision,host_incarnation) "
                "VALUES (%s,%s,%s,%s,%s)",
                (self.store.bot_id, operation_id, component_id, revision, lease.incarnation),
            )
            await self.store._host(conn, lease, runtime)

    async def admit_invocation(self, ticket: dict, operation_id: str, payload: dict) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self._guard(conn, ticket, "service.invoke", operation_id, payload)
            replay = await self._replay(conn, claims)
            if replay is not None:
                return replay
            result = {"operation_id": operation_id, "status": "ACCEPTED"}
            await self._time(conn, claims)
            await self._record(conn, claims, result)
            return result

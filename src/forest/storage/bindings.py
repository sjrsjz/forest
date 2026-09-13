"""Fixed-account Binding authority, separate from HTTP connection objects.

Lock order: component routes (if needed) -> binding route -> runtime -> host ->
conversation/run -> business records. Never perform network I/O in a transaction.
"""

from __future__ import annotations

import hashlib
import json
import re
from types import SimpleNamespace
from uuid import uuid4

from forest.drivers.protocol import validate_json
from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.bot import BotStore, HostLease
from forest.storage.config_payloads import ConfigPayloadStore
from forest.storage.control import BotControl


def _clone(value: dict) -> dict:
    """Freeze a caller-owned bounded DTO before any lock or pool await."""
    try:
        if not isinstance(value, dict):
            raise ValueError("Expected an object")
        validate_json(value)
        encoded = json.dumps(value, allow_nan=False, ensure_ascii=False).encode("utf-8")
        if len(encoded) > 64 * 1024:
            raise ValueError("DTO limit")
        return json.loads(encoded)
    except (ForestError, ValueError, TypeError, UnicodeError, RecursionError):
        raise ForestError("BINDING_INVALID", "Invalid bounded binding configuration", 422) from None


def _text(value: str, *, limit: int = 256) -> str:
    if not isinstance(value, str) or not 0 < len(value) <= limit:
        raise ForestError("BINDING_INVALID", "Invalid binding identity or operation", 422)
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise ForestError("BINDING_INVALID", "Invalid binding identity or operation", 422) from None
    return value


def _revision(value: int, *, minimum: int = 0) -> None:
    if type(value) is not int or not minimum <= value < 2**63:
        raise ForestError("BINDING_INVALID", "Invalid binding revision", 422)


class BindingStore:
    def __init__(self, store: BotStore):
        self.store = store
        self.control = BotControl(store)
        self.content = ConfigPayloadStore(store)

    @staticmethod
    async def current_config_in(
        conn,
        bot_id,
        binding_id,
        *,
        expected_binding_epoch=None,
        required=True,
    ):
        """Read the actual effective value after the caller's business locks.

        No Binding/Host lock is acquired here. Metadata/observation cleanup uses
        the ordinary authority guards without requiring this content reader.
        """
        row = await (
            await conn.execute(
                "SELECT b.binding_id,b.binding_epoch,b.connection_epoch,b.effective_revision,"
                "b.config_payload_id,b.config_payload_digest,r.values_payload_id,r.values_payload_digest "
                "FROM public.bot_bindings b LEFT JOIN public.bot_binding_revisions r ON "
                "(r.bot_id,r.binding_id,r.revision)=(b.bot_id,b.binding_id,b.effective_revision) "
                "WHERE b.bot_id=%s AND b.binding_id=%s",
                (bot_id, binding_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("BINDING_NOT_FOUND", "Binding does not belong to this Bot", 404)
        if expected_binding_epoch is not None and row["binding_epoch"] != expected_binding_epoch:
            raise ForestError("STALE_BINDING", "Binding configuration was replaced", 409)
        result = {
            key: row[key]
            for key in (
                "binding_id",
                "binding_epoch",
                "connection_epoch",
                "effective_revision",
                "config_payload_id",
                "config_payload_digest",
            )
        }
        if row["effective_revision"] == 0:
            if row["config_payload_id"] is not None or row["config_payload_digest"] is not None:
                raise ForestError(
                    "CONFIG_PAYLOAD_IDENTITY_CONFLICT",
                    "Initial Binding has an effective value",
                    409,
                )
            if required:
                raise ForestError(
                    "CONTROL_CONTENT_REVOKED", "Binding has no effective configuration", 409
                )
            return {**result, "config": None, "config_payload_state": "NOT_CONFIGURED"}
        if (row["config_payload_id"], row["config_payload_digest"]) != (
            row["values_payload_id"],
            row["values_payload_digest"],
        ) or row["values_payload_id"] is None:
            raise ForestError(
                "CONFIG_PAYLOAD_IDENTITY_CONFLICT", "Effective Binding reference differs", 409
            )
        content = ConfigPayloadStore(SimpleNamespace(bot_id=bot_id))
        epoch = (await content.payloads.lock_in(conn))["deletion_epoch"]
        saved = await content.hydrate_values_in(
            conn,
            row,
            family="binding",
            kind="REVISION",
            resource=binding_id,
            identity=str(row["effective_revision"]),
            epoch=epoch,
            required=required,
        )
        return {
            **result,
            "config": saved["values"],
            "config_payload_state": saved["values_payload_state"],
        }

    async def initial_in(self, conn, row, *, required=False):
        epoch = (await self.content.payloads.lock_in(conn))["deletion_epoch"]
        saved = await self.content.hydrate_values_in(
            conn,
            {
                "values_payload_id": row["initial_payload_id"],
                "values_payload_digest": row["initial_payload_digest"],
            },
            family="binding",
            kind="INITIAL",
            resource=row["binding_id"],
            identity=row["registry_operation_id"],
            epoch=epoch,
            required=required,
        )
        value = saved["values"]
        if saved["values_payload_state"] == "ACTIVE" and (
            not isinstance(value, dict)
            or set(value) != {"display_name"}
            or not isinstance(value["display_name"], str)
        ):
            raise ForestError(
                "CONFIG_PAYLOAD_IDENTITY_CONFLICT", "Binding initial value differs", 409
            )
        return {
            "display_name": value["display_name"] if value is not None else None,
            "initial_payload_id": row["initial_payload_id"],
            "initial_payload_digest": row["initial_payload_digest"],
            "display_name_payload_state": saved["values_payload_state"],
        }

    async def _values_in(self, conn, row, *, kind="REVISION", required=True):
        epoch = (await self.content.payloads.lock_in(conn))["deletion_epoch"]
        saved = await self.content.hydrate_values_in(
            conn,
            row,
            family="binding",
            kind=kind,
            resource=row["binding_id"],
            identity=row["plan_id"] if kind == "PLAN" else str(row["revision"]),
            epoch=epoch,
            required=required,
        )
        return {**row, **saved}

    async def _hydrate_in(self, conn, row):
        initial = await self.initial_in(conn, row)
        current = await self.current_config_in(
            conn, self.store.bot_id, row["binding_id"], required=False
        )
        return {**row, **initial, **current}

    async def _row(self, conn, binding_id: str) -> dict:
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_bindings WHERE bot_id=%s AND binding_id=%s FOR UPDATE",
                (self.store.bot_id, binding_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("BINDING_NOT_FOUND", "Binding does not belong to this Bot", 404)
        return row

    async def _guard(
        self,
        conn,
        lease: HostLease,
        binding_id: str,
        expected_binding_epoch: int | None = None,
        *,
        data: bool = True,
        require_receive: bool = False,
    ) -> dict:
        binding = await self._row(conn, binding_id)
        runtime = await self.store._runtime(conn)
        await self.store._host(conn, lease, runtime)
        if (
            expected_binding_epoch is not None
            and binding["binding_epoch"] != expected_binding_epoch
        ):
            raise ForestError("STALE_BINDING", "Binding configuration was replaced", 409)
        if (
            binding["host_incarnation"] != lease.incarnation
            or binding["host_owner_epoch"] != lease.owner_epoch
            or binding["state"] not in {"ONLINE", "DRAINING"}
        ):
            raise ForestError("BINDING_OFFLINE", "Binding has no current connection owner", 503)
        if data and (
            not runtime["data_admission"] or not binding["enabled"] or binding["state"] != "ONLINE"
        ):
            raise ForestError("DATA_ADMISSION_CLOSED", "Binding data admission is closed", 409)
        if not data and not runtime["control_admission"]:
            raise ForestError("CONTROL_CLOSED", "Bot control admission is closed", 409)
        if require_receive and not binding["receive_enabled"]:
            raise ForestError("RECEIVE_CLOSED", "Binding is not accepting events", 503)
        binding["_runtime"] = runtime
        return binding

    async def _final_guard(
        self,
        conn,
        lease: HostLease,
        binding: dict,
        *,
        data: bool = True,
        require_receive: bool = False,
        require_config: bool = False,
    ) -> None:
        runtime = binding["_runtime"]
        # All structural rows are still locked; only time can change under us.
        await self.store._host(conn, lease, runtime)
        if data and not runtime["data_admission"]:
            raise ForestError("DATA_ADMISSION_CLOSED", "Bot data admission is closed", 409)
        if require_receive and not binding["receive_enabled"]:
            raise ForestError("RECEIVE_CLOSED", "Binding is not accepting events", 503)
        if require_config:
            await self.current_config_in(
                conn,
                self.store.bot_id,
                binding["binding_id"],
                expected_binding_epoch=binding["binding_epoch"],
            )
            await self.store._host(conn, lease, runtime)

    async def create(self, ticket: dict, operation_id: str, payload: dict) -> dict:
        """Register an inactive binding after the deployment reserved its account."""
        ticket, payload = _clone(ticket), _clone(payload)
        _text(operation_id)
        if set(payload) != {"binding_id", "self_id", "display_name"}:
            raise ForestError("BINDING_INVALID", "Invalid binding registration", 422)
        for field in ("binding_id", "self_id", "display_name"):
            _text(payload[field])
        if (
            not re.fullmatch(r"[1-9][0-9]{0,18}", payload["self_id"])
            or int(payload["self_id"]) >= 2**63
        ):
            raise ForestError("BINDING_INVALID", "Invalid platform account identity", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            # A stable transaction lock serializes creation before a route row exists.
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("binding:" + self.store.bot_id + ":" + payload["binding_id"],),
            )
            claims = await self.control._guard(
                conn, ticket, "binding.create", operation_id, payload
            )
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            original = await self.control.admitted_input_in(conn, claims)
            epoch = (await self.content.payloads.lock_in(conn))["deletion_epoch"]
            initial = await self.content.put_values_in(
                conn,
                family="binding",
                kind="INITIAL",
                resource=payload["binding_id"],
                identity=operation_id,
                values={"display_name": payload["display_name"]},
                parents=(original.payload_id,),
                epoch=epoch,
            )
            inserted = await (
                await conn.execute(
                    "INSERT INTO public.bot_bindings(bot_id,binding_id,platform,self_id,display_name,registry_operation_id,"
                    "initial_payload_id,initial_payload_digest) "
                    "VALUES (%s,%s,'onebot-v11',%s,NULL,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING binding_id",
                    (
                        self.store.bot_id,
                        payload["binding_id"],
                        payload["self_id"],
                        operation_id,
                        initial.payload_id,
                        initial.digest,
                    ),
                )
            ).fetchone()
            if inserted is None:
                raise ForestError(
                    "BINDING_IDENTITY_CONFLICT",
                    "Binding or platform account is already registered",
                    409,
                )
            result = {"binding_id": payload["binding_id"], "revision": 0, "state": "DISABLED"}
            await self.control._record(conn, claims, result, parents=(initial.payload_id,))
            await self.control._time(conn, claims)
            return result

    async def list(self, ticket: dict, operation_id: str) -> list[dict]:
        ticket = _clone(ticket)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "binding.read", operation_id, {})
            rows = await (
                await conn.execute(
                    "SELECT * FROM public.bot_bindings WHERE bot_id=%s ORDER BY binding_id",
                    (self.store.bot_id,),
                )
            ).fetchall()
            await self.control._time(conn, claims)
            rows = [await self._hydrate_in(conn, row) for row in rows]
            await self.control._time(conn, claims)
            return [self.public(row) for row in rows]

    @staticmethod
    def public(row: dict) -> dict:
        return {
            key: row[key]
            for key in (
                "binding_id",
                "platform",
                "self_id",
                "display_name",
                "revision",
                "effective_revision",
                "binding_epoch",
                "connection_epoch",
                "enabled",
                "receive_enabled",
                "config",
                "state",
                "error_code",
                "display_name_payload_state",
                "config_payload_state",
            )
        }

    async def read(self, ticket: dict, operation_id: str, binding_id: str) -> dict:
        ticket = _clone(ticket)
        _text(binding_id)
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await self._row(conn, binding_id)
            claims = await self.control._guard(
                conn, ticket, "binding.read", operation_id, {"binding_id": binding_id}
            )
            desired = await (
                await conn.execute(
                    "SELECT * FROM public.bot_binding_revisions WHERE bot_id=%s AND binding_id=%s AND revision=%s",
                    (self.store.bot_id, binding_id, row["revision"]),
                )
            ).fetchone()
            await self.control._time(conn, claims)
            row = await self._hydrate_in(conn, row)
            desired = await self._values_in(conn, desired, required=False) if desired else None
            await self.control._time(conn, claims)
            return {
                **self.public(row),
                "desired_values": desired["values"] if desired else None,
                "desired_values_payload_state": desired["values_payload_state"]
                if desired
                else "NOT_CONFIGURED",
            }

    async def plan(
        self, ticket: dict, operation_id: str, payload: dict, secret_ciphertext: str
    ) -> dict:
        ticket, payload = _clone(ticket), _clone(payload)
        _text(operation_id)
        _text(secret_ciphertext, limit=32768)
        if set(payload) != {
            "binding_id",
            "expected_revision",
            "values",
            "credentials_changed",
            "secret_digest",
        }:
            raise ForestError("BINDING_INVALID", "Invalid binding plan", 422)
        _text(payload["binding_id"])
        _revision(payload["expected_revision"])
        if (
            not isinstance(payload["values"], dict)
            or type(payload["values"].get("enabled")) is not bool
            or type(payload["credentials_changed"]) is not bool
        ):
            raise ForestError(
                "BINDING_INVALID",
                "Binding plan requires explicit enable and credential decisions",
                422,
            )
        if (
            payload["secret_digest"]
            != hashlib.sha256(secret_ciphertext.encode("utf-8")).hexdigest()
        ):
            raise ForestError(
                "CREDENTIALS_CONFLICT", "Binding credentials differ from the signed request", 409
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await self._row(conn, payload["binding_id"])
            claims = await self.control._guard(conn, ticket, "binding.plan", operation_id, payload)
            await self.control._time(conn, claims)
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            if row["revision"] != payload["expected_revision"]:
                raise ForestError("REVISION_CONFLICT", "Binding settings changed; reload them", 409)
            plan_id = str(uuid4())
            plan_digest = digest_json(
                {
                    **payload,
                    "subject_id": claims["subject_id"],
                    "issuer_id": claims["issuer_id"],
                    "bot_id": self.store.bot_id,
                }
            )
            original = await self.control.admitted_input_in(conn, claims)
            epoch = (await self.content.payloads.lock_in(conn))["deletion_epoch"]
            values_ref = await self.content.put_values_in(
                conn,
                family="binding",
                kind="PLAN",
                resource=row["binding_id"],
                identity=plan_id,
                values=payload["values"],
                parents=(original.payload_id,),
                epoch=epoch,
            )
            await conn.execute(
                "INSERT INTO public.bot_binding_plans(bot_id,binding_id,plan_id,subject_id,issuer_id,expected_revision,"
                "values,secret_ciphertext,plan_digest,expires_at,values_payload_id,values_payload_digest,settings_enabled) "
                "VALUES (%s,%s,%s,%s,%s,%s,NULL,%s,%s,clock_timestamp()+interval '10 minutes',%s,%s,%s)",
                (
                    self.store.bot_id,
                    payload["binding_id"],
                    plan_id,
                    claims["subject_id"],
                    claims["issuer_id"],
                    row["revision"],
                    secret_ciphertext,
                    plan_digest,
                    values_ref.payload_id,
                    values_ref.digest,
                    payload["values"]["enabled"],
                ),
            )
            result = {
                "plan_id": plan_id,
                "plan_digest": plan_digest,
                "values": payload["values"],
                "expected_revision": row["revision"],
                "credentials_changed": payload["credentials_changed"],
            }
            await self.control._record(conn, claims, result, parents=(values_ref.payload_id,))
            await self.control._time(conn, claims)
            return result

    async def apply(self, ticket: dict, operation_id: str, payload: dict) -> dict:
        ticket, payload = _clone(ticket), _clone(payload)
        _text(operation_id)
        if set(payload) != {"binding_id", "expected_revision", "plan_id", "plan_digest"}:
            raise ForestError("BINDING_INVALID", "Invalid binding apply request", 422)
        for field in ("binding_id", "plan_id", "plan_digest"):
            _text(payload[field])
        _revision(payload["expected_revision"])
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await self._row(conn, payload["binding_id"])
            claims = await self.control._guard(
                conn, ticket, "binding.apply", operation_id, payload, payload["plan_digest"]
            )
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            plan = await (
                await conn.execute(
                    "SELECT * FROM public.bot_binding_plans WHERE bot_id=%s AND plan_id=%s",
                    (self.store.bot_id, payload["plan_id"]),
                )
            ).fetchone()
            if plan is None or any(plan[k] != claims[k] for k in ("subject_id", "issuer_id")):
                raise ForestError("PLAN_NOT_FOUND", "Binding plan is unavailable", 404)
            if any(
                plan[k] != payload[k] for k in ("binding_id", "plan_digest", "expected_revision")
            ):
                raise ForestError("PLAN_CONFLICT", "Binding plan does not match", 409)
            if row["revision"] != payload["expected_revision"]:
                raise ForestError(
                    "REVISION_CONFLICT", "Binding was changed in another request", 409
                )
            if plan["expires_at"] <= await self.store._now(conn):
                raise ForestError("PLAN_EXPIRED", "Preview the binding settings again", 409)
            revision = row["revision"] + 1
            parent = await self.control.plan_parent(conn, "binding", plan)
            plan = await self._values_in(conn, plan, kind="PLAN")
            original = await self.control.admitted_input_in(conn, claims)
            epoch = (await self.content.payloads.lock_in(conn))["deletion_epoch"]
            values_ref = await self.content.put_values_in(
                conn,
                family="binding",
                kind="REVISION",
                resource=row["binding_id"],
                identity=str(revision),
                values=plan["values"],
                parents=(original.payload_id, parent),
                epoch=epoch,
            )
            result = {
                "binding_id": row["binding_id"],
                "revision": revision,
                "effective_revision": row["effective_revision"],
                "status": "ACCEPTED",
            }
            await self.control._record(conn, claims, result, parents=(values_ref.payload_id,))
            await conn.execute(
                "INSERT INTO public.bot_binding_revisions(bot_id,binding_id,revision,previous_revision,plan_id,"
                "values,secret_ciphertext,policy_revision,operation_id,values_payload_id,values_payload_digest,settings_enabled) "
                "VALUES (%s,%s,%s,%s,%s,NULL,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    row["binding_id"],
                    revision,
                    row["revision"],
                    plan["plan_id"],
                    plan["secret_ciphertext"],
                    claims["policy_revision"],
                    operation_id,
                    values_ref.payload_id,
                    values_ref.digest,
                    plan["values"]["enabled"],
                ),
            )
            await conn.execute(
                "UPDATE public.bot_bindings SET revision=%s,error_code=NULL WHERE bot_id=%s AND binding_id=%s",
                (revision, self.store.bot_id, row["binding_id"]),
            )
            if plan["expires_at"] <= await self.store._now(conn):
                raise ForestError("PLAN_EXPIRED", "Binding plan expired before publication", 409)
            await self.control._time(conn, claims)
            return result

    async def desired(self, lease: HostLease) -> list[dict]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            rows = await (
                await conn.execute(
                    "SELECT b.* "
                    "FROM public.bot_bindings b JOIN public.bot_binding_revisions r ON "
                    "(r.bot_id,r.binding_id,r.revision)=(b.bot_id,b.binding_id,b.revision) WHERE b.bot_id=%s ORDER BY b.binding_id",
                    (self.store.bot_id,),
                )
            ).fetchall()
            results = []
            for row in rows:
                revisions = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_binding_revisions WHERE bot_id=%s AND binding_id=%s AND revision=ANY(%s)",
                        (
                            self.store.bot_id,
                            row["binding_id"],
                            [row["revision"], row["effective_revision"]],
                        ),
                    )
                ).fetchall()
                saved = {
                    r["revision"]: await self._values_in(conn, r, required=False) for r in revisions
                }
                desired = saved[row["revision"]]
                effective = saved.get(row["effective_revision"])
                results.append(
                    {
                        **await self._hydrate_in(conn, row),
                        "desired_values": desired["values"],
                        "desired_values_payload_state": desired["values_payload_state"],
                        "desired_policy_revision": desired["policy_revision"],
                        "secret_ciphertext": desired["secret_ciphertext"],
                        "effective_values": effective["values"] if effective else None,
                        "effective_values_payload_state": effective["values_payload_state"]
                        if effective
                        else "NOT_CONFIGURED",
                        "effective_secret_ciphertext": effective["secret_ciphertext"]
                        if effective
                        else None,
                    }
                )
            await self.store._host(conn, lease)
            return results

    async def execution_config(self, lease, binding_id, *, expected_binding_epoch, data=False):
        async with self.store.pool.connection() as conn, conn.transaction():
            binding = await self._guard(conn, lease, binding_id, expected_binding_epoch, data=data)
            value = await self.current_config_in(
                conn,
                self.store.bot_id,
                binding_id,
                expected_binding_epoch=expected_binding_epoch,
            )
            await self._final_guard(conn, lease, binding, data=data)
            return value

    async def candidate_config(
        self, lease, binding_id, revision, *, expected_desired_revision, recover_effective=False
    ):
        """Fresh value and ciphertext before a candidate performs any HTTP I/O."""
        async with self.store.pool.connection() as conn, conn.transaction():
            binding = await self._row(conn, binding_id)
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            if binding["revision"] != expected_desired_revision or revision != (
                binding["effective_revision"] if recover_effective else binding["revision"]
            ):
                raise ForestError("CONFIGURATION_SUPERSEDED", "Binding candidate was replaced", 409)
            saved = await (
                await conn.execute(
                    "SELECT * FROM public.bot_binding_revisions WHERE bot_id=%s AND binding_id=%s AND revision=%s",
                    (self.store.bot_id, binding_id, revision),
                )
            ).fetchone()
            if (
                saved is None
                or saved["policy_revision"] != runtime["policy_revision"]
                or not runtime["control_admission"]
            ):
                raise ForestError(
                    "POLICY_REVOKED", "Binding candidate requires current authorization", 403
                )
            saved = await self._values_in(conn, saved)
            await self.store._host(conn, lease, runtime)
            return saved

    async def credentials(self, lease: HostLease, binding_id: str) -> str | None:
        """Host only; returns ciphertext for current desired revision, never a public DTO."""
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await self._row(conn, binding_id)
            await self.store._host(conn, lease)
            secret = await (
                await conn.execute(
                    "SELECT secret_ciphertext FROM public.bot_binding_revisions WHERE bot_id=%s AND binding_id=%s AND revision=%s",
                    (self.store.bot_id, binding_id, row["revision"]),
                )
            ).fetchone()
            await self.store._host(conn, lease)
            return secret["secret_ciphertext"] if secret else None

    async def current(self, lease: HostLease, binding_id: str) -> dict:
        """Host-only publication reconciliation, serialized with route changes."""
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await self._row(conn, binding_id)
            await self.store._host(conn, lease)
            row = await self._hydrate_in(conn, row)
            await self.store._host(conn, lease)
            return row

    async def disconnect(
        self,
        lease: HostLease,
        binding_id: str,
        *,
        client_incarnation: str,
        expected_binding_epoch: int,
    ) -> dict:
        """Fence the exact retiring client before closing its owned transport."""
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await self._row(conn, binding_id)
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            if (
                row["client_incarnation"] != client_incarnation
                or row["binding_epoch"] != expected_binding_epoch
                or row["host_incarnation"] != lease.incarnation
                or row["host_owner_epoch"] != lease.owner_epoch
            ):
                raise ForestError(
                    "STALE_BINDING", "Retiring client no longer owns this binding", 409
                )
            if row["state"] in {"OFFLINE", "DISABLED"}:
                await self.store._host(conn, lease, runtime)
                return row
            row = await (
                await conn.execute(
                    "UPDATE public.bot_bindings SET state='OFFLINE',enabled=false,receive_enabled=false "
                    "WHERE bot_id=%s AND binding_id=%s RETURNING *",
                    (self.store.bot_id, binding_id),
                )
            ).fetchone()
            await self.store._event(
                conn,
                "binding.disconnected",
                None,
                {"binding_id": binding_id, "connection_epoch": row["connection_epoch"]},
            )
            await self.store._host(conn, lease, runtime)
            return row

    async def stage_credentials(
        self,
        lease: HostLease,
        binding_id: str,
        subject_id: str,
        operation_id: str,
        plaintext_digest: str,
        candidate_ciphertext: str,
    ) -> str:
        """Freeze one encryption result for a currently authenticated host request.

        The gateway checks the current Web session before staging, then signs a
        plan binding the returned ciphertext digest. Staging alone authorizes no
        publication or platform access. A retry never creates a new random
        ciphertext for the same credential intent.
        """
        for value in (binding_id, subject_id, operation_id):
            _text(value)
        _text(candidate_ciphertext, limit=32768)
        if not isinstance(plaintext_digest, str) or not re.fullmatch(
            r"(?:sha256:)?[0-9a-f]{64}", plaintext_digest
        ):
            raise ForestError("BINDING_INVALID", "Invalid credential input digest", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._row(conn, binding_id)
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_binding_credentials WHERE bot_id=%s AND operation_id=%s",
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if row is not None:
                if (row["binding_id"], row["subject_id"], row["plaintext_digest"]) != (
                    binding_id,
                    subject_id,
                    plaintext_digest,
                ):
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Credential operation has different input", 409
                    )
                await self.store._host(conn, lease, runtime)
                return row["secret_ciphertext"]
            await conn.execute(
                "INSERT INTO public.bot_binding_credentials(bot_id,operation_id,binding_id,subject_id,"
                "plaintext_digest,secret_ciphertext) VALUES (%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    operation_id,
                    binding_id,
                    subject_id,
                    plaintext_digest,
                    candidate_ciphertext,
                ),
            )
            await self.store._host(conn, lease, runtime)
            return candidate_ciphertext

    async def drain(self, lease: HostLease, binding_id: str, expected_revision: int) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await self._row(conn, binding_id)
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            if row["revision"] != expected_revision:
                raise ForestError(
                    "CONFIGURATION_SUPERSEDED", "A newer binding configuration is pending", 409
                )
            await conn.execute(
                "UPDATE public.bot_bindings SET state='DRAINING' WHERE bot_id=%s AND binding_id=%s",
                (self.store.bot_id, binding_id),
            )
            await self.store._host(conn, lease, runtime)
            return row

    async def activate(
        self,
        lease: HostLease,
        binding_id: str,
        revision: int,
        *,
        enabled: bool,
        client_incarnation: str,
        expected_binding_epoch: int,
        recover_effective: bool = False,
        expected_desired_revision: int | None = None,
    ) -> dict:
        """Publish after candidate identity probe and old HTTP client termination."""
        _text(binding_id)
        _text(client_incarnation)
        _revision(revision, minimum=1)
        _revision(expected_binding_epoch, minimum=1)
        if type(enabled) is not bool or type(recover_effective) is not bool:
            raise ForestError(
                "BINDING_INVALID", "Binding activation requires an enable decision", 422
            )
        if recover_effective:
            _revision(expected_desired_revision, minimum=1)
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await self._row(conn, binding_id)
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            saved = await (
                await conn.execute(
                    "SELECT * FROM public.bot_binding_revisions WHERE bot_id=%s AND binding_id=%s AND revision=%s",
                    (self.store.bot_id, binding_id, revision),
                )
            ).fetchone()
            valid_revision = (
                row["effective_revision"] == revision
                and row["revision"] == expected_desired_revision
                if recover_effective
                else row["revision"] == revision
            )
            if saved is None or not valid_revision:
                raise ForestError(
                    "CONFIGURATION_SUPERSEDED", "A newer binding configuration is pending", 409
                )
            if (
                saved["policy_revision"] != runtime["policy_revision"]
                or not runtime["control_admission"]
            ):
                raise ForestError(
                    "POLICY_REVOKED", "Binding settings require current authorization", 403
                )
            saved = await self._values_in(conn, saved)
            if saved["values"]["enabled"] is not enabled:
                raise ForestError(
                    "CONFIGURATION_CONFLICT",
                    "Binding activation differs from desired settings",
                    409,
                )
            if row["client_incarnation"] == client_incarnation:
                if (
                    row["host_incarnation"] != lease.incarnation
                    or row["host_owner_epoch"] != lease.owner_epoch
                    or row["effective_revision"] != revision
                    or row["binding_epoch"] != expected_binding_epoch + 1
                ):
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Connection activation has different ownership", 409
                    )
                row = await self._hydrate_in(conn, row)
                await self.store._host(conn, lease, runtime)
                return row
            if row["binding_epoch"] != expected_binding_epoch:
                raise ForestError("STALE_BINDING", "Binding connection was replaced", 409)
            used = await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_binding_clients WHERE bot_id=%s AND client_incarnation=%s",
                    (self.store.bot_id, client_incarnation),
                )
            ).fetchone()
            if used:
                raise ForestError(
                    "IDEMPOTENCY_CONFLICT", "Retired client incarnation cannot be reused", 409
                )
            await conn.execute(
                "INSERT INTO public.bot_binding_clients(bot_id,client_incarnation,binding_id,revision,"
                "binding_epoch,connection_epoch,host_incarnation,host_owner_epoch) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    client_incarnation,
                    binding_id,
                    revision,
                    row["binding_epoch"] + 1,
                    row["connection_epoch"] + 1,
                    lease.incarnation,
                    lease.owner_epoch,
                ),
            )
            result = await (
                await conn.execute(
                    "UPDATE public.bot_bindings SET effective_revision=%s,binding_epoch=binding_epoch+1,"
                    "connection_epoch=connection_epoch+1,host_incarnation=%s,host_owner_epoch=%s,enabled=%s,"
                    "receive_enabled=%s,config=NULL,config_payload_id=%s,config_payload_digest=%s,"
                    "state=%s,client_incarnation=%s,error_code=NULL WHERE bot_id=%s AND binding_id=%s RETURNING *",
                    (
                        revision,
                        lease.incarnation,
                        lease.owner_epoch,
                        enabled,
                        enabled,
                        saved["values_payload_id"],
                        saved["values_payload_digest"],
                        "ONLINE" if enabled else "DISABLED",
                        client_incarnation,
                        self.store.bot_id,
                        binding_id,
                    ),
                )
            ).fetchone()
            await self.store._event(
                conn,
                "binding.activated",
                saved["operation_id"],
                {
                    "binding_id": binding_id,
                    "revision": revision,
                    "connection_epoch": result["connection_epoch"],
                },
            )
            result = await self._hydrate_in(conn, result)
            await self.store._host(conn, lease, runtime)
            return result

    async def report_error(
        self, lease: HostLease, binding_id: str, revision: int, code: str
    ) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await self._row(conn, binding_id)
            await self.store._host(conn, lease)
            if row["revision"] == revision:
                await conn.execute(
                    "UPDATE public.bot_bindings SET error_code=%s WHERE bot_id=%s AND binding_id=%s",
                    (code, self.store.bot_id, binding_id),
                )
            await self.store._host(conn, lease)

"""Deployment-scoped Bot catalog and fenced supervisor observations."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from importlib.resources import files

import psycopg

from forest.errors import ForestError
from forest.packages import digest_json

_ERROR = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
_DIGEST = re.compile(r"sha256:[a-f0-9]{64}\Z")


def _bounded(value: object, field: str, limit: int) -> str:
    if not isinstance(value, str):
        raise ForestError("DEPLOYMENT_INPUT_INVALID", f"Invalid {field}", 422)
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError:
        size = 0
    if not 1 <= size <= limit or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ForestError("DEPLOYMENT_INPUT_INVALID", f"Invalid {field}", 422)
    return value


@dataclass(frozen=True)
class DeploymentSupervisorLease:
    deployment_id: str
    host_id: str
    incarnation: str
    owner_epoch: int
    expires_at: datetime


class DeploymentCatalogRepository:
    """Own stable Bot identities, runtime intent and supervisor observations."""

    def __init__(self, store) -> None:
        self.store = store

    async def initialize(self) -> dict:
        source = files("forest.storage.official").joinpath("deployment_catalog.sql").read_bytes()
        baseline_sha256 = hashlib.sha256(source).hexdigest()
        try:
            statement = source.decode("utf-8")
        except UnicodeError as exc:  # pragma: no cover - packaged source invariant
            raise ForestError("DEPLOYMENT_BASELINE_INVALID", "Baseline is not UTF-8", 500) from exc
        async with self.store.pool.connection() as conn:
            try:
                async with conn.transaction():
                    await conn.execute("SET LOCAL lock_timeout='5s'")
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                        ("forest.deployment.catalog/1",),
                    )
                    relation = await (
                        await conn.execute(
                            "SELECT to_regclass('forest_deployment.deployment_identity') AS value"
                        )
                    ).fetchone()
                    if relation["value"] is not None:
                        identity = await (
                            await conn.execute(
                                "SELECT * FROM forest_deployment.deployment_identity "
                                "WHERE singleton FOR SHARE"
                            )
                        ).fetchone()
                        if (
                            identity is None
                            or identity["deployment_id"] != self.store.deployment_id
                            or identity["baseline_sha256"] != baseline_sha256
                        ):
                            raise ForestError(
                                "DEPLOYMENT_SCOPE_MISMATCH",
                                "Deployment database identity differs",
                                409,
                            )
                        await self._verify_inventory(conn)
                        return dict(identity)
                    await self._require_empty(conn)
                    await conn.execute(statement)
                    identity = await (
                        await conn.execute(
                            "INSERT INTO forest_deployment.deployment_identity("
                            "deployment_id,baseline_sha256) VALUES (%s,%s) RETURNING *",
                            (self.store.deployment_id, baseline_sha256),
                        )
                    ).fetchone()
                    await self._verify_inventory(conn)
                    return dict(identity)
            except ForestError:
                raise
            except psycopg.Error as exc:
                raise ForestError(
                    "DEPLOYMENT_BASELINE_INSTALL_FAILED",
                    "Deployment catalog could not be installed atomically",
                    503,
                ) from exc

    @staticmethod
    async def _require_empty(conn) -> None:
        row = await (
            await conn.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname NOT IN "
                "('pg_catalog','information_schema','public') AND nspname NOT LIKE 'pg_toast%' "
                "AND nspname NOT LIKE 'pg_temp_%' AND nspname NOT LIKE 'pg_toast_temp_%') "
                "OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S','f')) "
                "OR EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                "WHERE n.nspname='public') AS occupied"
            )
        ).fetchone()
        if row["occupied"]:
            raise ForestError(
                "DEPLOYMENT_DATABASE_NOT_EMPTY",
                "Deployment catalog requires an empty dedicated database",
                409,
            )

    @staticmethod
    async def _verify_inventory(conn) -> None:
        relations = await (
            await conn.execute(
                "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='forest_deployment' AND c.relkind IN ('r','p','v','m','S','f')"
            )
        ).fetchall()
        expected_relations = {
            "deployment_identity",
            "supervisor_lease",
            "bots",
            "runtime_intents",
            "runtime_observations",
            "operations",
            "secret_slots",
            "secret_versions",
            "storage_profiles",
            "storage_profile_operations",
            "bootstrap_profiles",
            "bootstrap_profile_components",
            "bootstrap_profile_operations",
            "provisioning_jobs",
            "provisioning_operations",
        }
        functions = await (
            await conn.execute(
                "SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                "WHERE n.nspname='forest_deployment'"
            )
        ).fetchall()
        if {row["relname"] for row in relations} != expected_relations or {
            row["proname"] for row in functions
        } != {
            "reject_identity_change",
            "guard_bot_change",
            "guard_revisioned_change",
            "guard_secret_slot_change",
            "guard_provisioning_job_change",
        }:
            raise ForestError(
                "DEPLOYMENT_SCHEMA_TAMPERED",
                "Deployment catalog contains missing or unknown objects",
                409,
            )

    async def register_ready(
        self,
        *,
        operation_id: str,
        bot_id: str,
        display_name: str,
        storage_profile_ref: str,
        bootstrap_profile_ref: str,
        bootstrap_manifest_digest: str,
        storage_generation: int = 1,
    ) -> dict:
        for value, field, limit in (
            (operation_id, "operation_id", 256),
            (bot_id, "bot_id", 128),
            (display_name, "display_name", 256),
            (storage_profile_ref, "storage_profile_ref", 512),
            (bootstrap_profile_ref, "bootstrap_profile_ref", 512),
            (bootstrap_manifest_digest, "bootstrap_manifest_digest", 71),
        ):
            _bounded(value, field, limit)
        if type(storage_generation) is not int or storage_generation < 1:
            raise ForestError("DEPLOYMENT_INPUT_INVALID", "Storage generation is invalid", 422)
        if _DIGEST.fullmatch(bootstrap_manifest_digest) is None:
            raise ForestError("DEPLOYMENT_INPUT_INVALID", "Bootstrap digest is invalid", 422)
        input_digest = digest_json(
            {
                "bot_id": bot_id,
                "display_name": display_name,
                "storage_profile_ref": storage_profile_ref,
                "storage_generation": storage_generation,
                "bootstrap_profile_ref": bootstrap_profile_ref,
                "bootstrap_manifest_digest": bootstrap_manifest_digest,
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            replay = await self._operation(conn, operation_id, input_digest)
            if replay is not None:
                return await self._bot_in(conn, replay["bot_id"])
            profile = await (
                await conn.execute(
                    "SELECT bot_id,storage_generation FROM forest_deployment.storage_profiles "
                    "WHERE profile_ref=%s FOR SHARE",
                    (storage_profile_ref,),
                )
            ).fetchone()
            if profile is None:
                raise ForestError(
                    "STORAGE_PROFILE_NOT_FOUND", "Storage profile does not exist", 404
                )
            if (
                profile["bot_id"] != bot_id
                or profile["storage_generation"] != storage_generation
            ):
                raise ForestError(
                    "STORAGE_PROFILE_SCOPE_MISMATCH",
                    "Storage profile belongs to another Bot or generation",
                    409,
                )
            bootstrap = await (
                await conn.execute(
                    "SELECT manifest_digest FROM forest_deployment.bootstrap_profiles "
                    "WHERE profile_ref=%s AND bot_id=%s AND storage_profile_ref=%s "
                    "AND storage_generation=%s FOR SHARE",
                    (bootstrap_profile_ref, bot_id, storage_profile_ref, storage_generation),
                )
            ).fetchone()
            if bootstrap is None or bootstrap["manifest_digest"] != bootstrap_manifest_digest:
                raise ForestError(
                    "BOOTSTRAP_PROFILE_SCOPE_MISMATCH",
                    "Bootstrap profile differs from the ready Bot identity",
                    409,
                )
            existing = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.bots WHERE bot_id=%s FOR UPDATE",
                    (bot_id,),
                )
            ).fetchone()
            if existing is not None:
                raise ForestError("BOT_IDENTITY_EXISTS", "Bot identity cannot be reused", 409)
            await conn.execute(
                "INSERT INTO forest_deployment.bots(bot_id,display_name,supply_state,"
                "storage_profile_ref,storage_generation,bootstrap_profile_ref,"
                "bootstrap_manifest_digest) VALUES (%s,%s,'READY',%s,%s,%s,%s)",
                (
                    bot_id,
                    display_name,
                    storage_profile_ref,
                    storage_generation,
                    bootstrap_profile_ref,
                    bootstrap_manifest_digest,
                ),
            )
            await conn.execute(
                "INSERT INTO forest_deployment.runtime_intents(bot_id,desired_state) "
                "VALUES (%s,'OFFLINE')",
                (bot_id,),
            )
            await self._save_operation(conn, operation_id, "REGISTER_READY", input_digest, bot_id, 1)
            return await self._bot_in(conn, bot_id)

    async def set_desired(
        self,
        *,
        operation_id: str,
        bot_id: str,
        desired_state: str,
        expected_revision: int,
    ) -> dict:
        _bounded(operation_id, "operation_id", 256)
        _bounded(bot_id, "bot_id", 128)
        if desired_state not in {"OFFLINE", "ONLINE"} or type(expected_revision) is not int:
            raise ForestError("DEPLOYMENT_INPUT_INVALID", "Runtime intent is invalid", 422)
        input_digest = digest_json(
            {
                "bot_id": bot_id,
                "desired_state": desired_state,
                "expected_revision": expected_revision,
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            replay = await self._operation(conn, operation_id, input_digest)
            if replay is not None:
                return await self._bot_in(conn, replay["bot_id"])
            current = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.runtime_intents WHERE bot_id=%s FOR UPDATE",
                    (bot_id,),
                )
            ).fetchone()
            if current is None:
                raise ForestError("BOT_NOT_FOUND", "Bot identity does not exist", 404)
            if current["revision"] != expected_revision:
                raise ForestError(
                    "BOT_INTENT_REVISION_CONFLICT",
                    "Bot runtime intent changed",
                    409,
                    details={"revision": current["revision"]},
                )
            saved = await (
                await conn.execute(
                    "UPDATE forest_deployment.runtime_intents SET desired_state=%s,"
                    "revision=revision+1,updated_at=clock_timestamp() WHERE bot_id=%s RETURNING *",
                    (desired_state, bot_id),
                )
            ).fetchone()
            await self._save_operation(
                conn, operation_id, "SET_DESIRED", input_digest, bot_id, saved["revision"]
            )
            return await self._bot_in(conn, bot_id)

    async def claim_supervisor(
        self, host_id: str, incarnation: str, *, lease_seconds: int = 30
    ) -> DeploymentSupervisorLease:
        _bounded(host_id, "host_id", 256)
        _bounded(incarnation, "incarnation", 256)
        if type(lease_seconds) is not int or not 3 <= lease_seconds <= 300:
            raise ForestError("DEPLOYMENT_INPUT_INVALID", "Supervisor lease is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    "SELECT *,expires_at<=clock_timestamp() AS expired "
                    "FROM forest_deployment.supervisor_lease WHERE singleton FOR UPDATE"
                )
            ).fetchone()
            same = row["host_id"] == host_id and row["incarnation"] == incarnation
            if not row["expired"] and not same:
                raise ForestError("DEPLOYMENT_SUPERVISOR_BUSY", "Another supervisor is active", 409)
            epoch = (
                row["owner_epoch"]
                if same and not row["expired"]
                else row["owner_epoch"] + 1
            )
            saved = await (
                await conn.execute(
                    "UPDATE forest_deployment.supervisor_lease SET host_id=%s,incarnation=%s,"
                    "owner_epoch=%s,expires_at=clock_timestamp()+make_interval(secs => %s) "
                    "WHERE singleton RETURNING *",
                    (host_id, incarnation, epoch, lease_seconds),
                )
            ).fetchone()
            return self._lease(saved)

    async def renew_supervisor(
        self, lease: DeploymentSupervisorLease, *, lease_seconds: int = 30
    ) -> DeploymentSupervisorLease:
        if (
            type(lease) is not DeploymentSupervisorLease
            or lease.deployment_id != self.store.deployment_id
            or type(lease_seconds) is not int
            or not 3 <= lease_seconds <= 300
        ):
            raise ForestError("DEPLOYMENT_INPUT_INVALID", "Supervisor lease is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            saved = await (
                await conn.execute(
                    "UPDATE forest_deployment.supervisor_lease SET expires_at=clock_timestamp()+"
                    "make_interval(secs => %s) WHERE singleton AND host_id=%s AND incarnation=%s "
                    "AND owner_epoch=%s AND expires_at>clock_timestamp() RETURNING *",
                    (lease_seconds, lease.host_id, lease.incarnation, lease.owner_epoch),
                )
            ).fetchone()
            if saved is None:
                raise ForestError("STALE_DEPLOYMENT_SUPERVISOR", "Supervisor lease is stale", 409)
            return self._lease(saved)

    async def release_supervisor(self, lease: DeploymentSupervisorLease) -> None:
        if (
            type(lease) is not DeploymentSupervisorLease
            or lease.deployment_id != self.store.deployment_id
        ):
            raise ForestError("DEPLOYMENT_INPUT_INVALID", "Supervisor lease is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            saved = await (
                await conn.execute(
                    "UPDATE forest_deployment.supervisor_lease SET expires_at=clock_timestamp() "
                    "WHERE singleton AND host_id=%s AND incarnation=%s AND owner_epoch=%s "
                    "RETURNING owner_epoch",
                    (lease.host_id, lease.incarnation, lease.owner_epoch),
                )
            ).fetchone()
            if saved is None:
                raise ForestError("STALE_DEPLOYMENT_SUPERVISOR", "Supervisor lease is stale", 409)

    async def observe(
        self,
        lease: DeploymentSupervisorLease,
        *,
        bot_id: str,
        observed_state: str,
        graph_id: str | None = None,
        error_code: str | None = None,
        retry_seconds: int = 5,
    ) -> dict:
        _bounded(bot_id, "bot_id", 128)
        if observed_state not in {"OFFLINE", "STARTING", "ONLINE", "STOPPING", "FAILED"}:
            raise ForestError("DEPLOYMENT_INPUT_INVALID", "Runtime observation is invalid", 422)
        if (observed_state == "ONLINE") != (graph_id is not None):
            raise ForestError("DEPLOYMENT_INPUT_INVALID", "Runtime graph observation is invalid", 422)
        if (observed_state == "FAILED") != (error_code is not None):
            raise ForestError("DEPLOYMENT_INPUT_INVALID", "Runtime failure observation is invalid", 422)
        if graph_id is not None:
            _bounded(graph_id, "graph_id", 256)
        if error_code is not None and _ERROR.fullmatch(error_code) is None:
            raise ForestError("DEPLOYMENT_INPUT_INVALID", "Runtime error code is invalid", 422)
        if type(retry_seconds) is not int or not 1 <= retry_seconds <= 3600:
            raise ForestError("DEPLOYMENT_INPUT_INVALID", "Retry delay is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._verify_lease_in(conn, lease)
            exists = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.bots WHERE bot_id=%s AND supply_state='READY'",
                    (bot_id,),
                )
            ).fetchone()
            if exists is None:
                raise ForestError("BOT_NOT_FOUND", "Ready Bot identity does not exist", 404)
            saved = await (
                await conn.execute(
                    "INSERT INTO forest_deployment.runtime_observations("
                    "bot_id,observed_state,supervisor_owner_epoch,supervisor_incarnation,graph_id,"
                    "error_code,consecutive_failures,retry_not_before) VALUES ("
                    "%s,%s,%s,%s,%s,%s,CASE WHEN %s='FAILED' THEN 1 ELSE 0 END,"
                    "CASE WHEN %s='FAILED' THEN clock_timestamp()+make_interval(secs => %s) END) "
                    "ON CONFLICT(bot_id) DO UPDATE SET observed_state=excluded.observed_state,"
                    "supervisor_owner_epoch=excluded.supervisor_owner_epoch,"
                    "supervisor_incarnation=excluded.supervisor_incarnation,graph_id=excluded.graph_id,"
                    "error_code=excluded.error_code,consecutive_failures=CASE "
                    "WHEN excluded.observed_state='FAILED' THEN "
                    "forest_deployment.runtime_observations.consecutive_failures+1 ELSE 0 END,"
                    "retry_not_before=CASE WHEN excluded.observed_state='FAILED' THEN "
                    "clock_timestamp()+make_interval(secs => %s) END,revision="
                    "forest_deployment.runtime_observations.revision+1,updated_at=clock_timestamp() "
                    "RETURNING *",
                    (
                        bot_id,
                        observed_state,
                        lease.owner_epoch,
                        lease.incarnation,
                        graph_id,
                        error_code,
                        observed_state,
                        observed_state,
                        retry_seconds,
                        retry_seconds,
                    ),
                )
            ).fetchone()
            return dict(saved)

    async def list_bots(self) -> tuple[dict, ...]:
        async with self.store.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT b.*,i.desired_state,i.revision AS intent_revision,"
                    "o.observed_state,o.graph_id,o.error_code,o.retry_not_before,"
                    "o.revision AS observation_revision FROM forest_deployment.bots b "
                    "JOIN forest_deployment.runtime_intents i USING(bot_id) "
                    "LEFT JOIN forest_deployment.runtime_observations o USING(bot_id) "
                    "ORDER BY b.created_at,b.bot_id"
                )
            ).fetchall()
            return tuple(dict(row) for row in rows)

    async def inspect(self, bot_id: str) -> dict:
        _bounded(bot_id, "bot_id", 128)
        async with self.store.pool.connection() as conn:
            return await self._bot_in(conn, bot_id)

    async def _bot_in(self, conn, bot_id: str) -> dict:
        row = await (
            await conn.execute(
                "SELECT b.*,i.desired_state,i.revision AS intent_revision,"
                "o.observed_state,o.graph_id,o.error_code,o.retry_not_before,"
                "o.revision AS observation_revision FROM forest_deployment.bots b "
                "JOIN forest_deployment.runtime_intents i USING(bot_id) "
                "LEFT JOIN forest_deployment.runtime_observations o USING(bot_id) "
                "WHERE b.bot_id=%s",
                (bot_id,),
            )
        ).fetchone()
        if row is None:
            raise ForestError("BOT_NOT_FOUND", "Bot identity does not exist", 404)
        return dict(row)

    async def _operation(self, conn, operation_id: str, input_digest: str):
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"forest.deployment.operation/{operation_id}",),
        )
        row = await (
            await conn.execute(
                "SELECT * FROM forest_deployment.operations WHERE operation_id=%s",
                (operation_id,),
            )
        ).fetchone()
        if row is not None and row["input_digest"] != input_digest:
            raise ForestError("DEPLOYMENT_OPERATION_CONFLICT", "Operation input differs", 409)
        return row

    @staticmethod
    async def _save_operation(
        conn, operation_id: str, kind: str, input_digest: str, bot_id: str, revision: int
    ) -> None:
        await conn.execute(
            "INSERT INTO forest_deployment.operations(operation_id,operation_kind,input_digest,"
            "bot_id,resulting_revision) VALUES (%s,%s,%s,%s,%s)",
            (operation_id, kind, input_digest, bot_id, revision),
        )

    async def _verify_lease_in(self, conn, lease: DeploymentSupervisorLease) -> None:
        if (
            type(lease) is not DeploymentSupervisorLease
            or lease.deployment_id != self.store.deployment_id
        ):
            raise ForestError("DEPLOYMENT_INPUT_INVALID", "Supervisor lease is invalid", 422)
        row = await (
            await conn.execute(
                "SELECT 1 FROM forest_deployment.supervisor_lease WHERE singleton "
                "AND host_id=%s AND incarnation=%s AND owner_epoch=%s "
                "AND expires_at>clock_timestamp() FOR SHARE",
                (lease.host_id, lease.incarnation, lease.owner_epoch),
            )
        ).fetchone()
        if row is None:
            raise ForestError("STALE_DEPLOYMENT_SUPERVISOR", "Supervisor lease is stale", 409)

    def _lease(self, row) -> DeploymentSupervisorLease:
        return DeploymentSupervisorLease(
            self.store.deployment_id,
            row["host_id"],
            row["incarnation"],
            row["owner_epoch"],
            row["expires_at"],
        )

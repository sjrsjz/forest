"""Immutable deployment storage profiles resolved through the Secret authority."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from forest.control_action_authorization import authorize_control_action_in
from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.deployment_catalog import (
    DeploymentCatalogRepository,
    verify_deployment_supervisor_in,
)

_PROFILE_REF = re.compile(r"storage-profile/[A-Za-z0-9][A-Za-z0-9._/-]{0,495}\Z")


def _bounded(value: object, field_name: str, limit: int) -> str:
    if not isinstance(value, str):
        raise ForestError("STORAGE_PROFILE_INPUT_INVALID", f"Invalid {field_name}", 422)
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError:
        size = 0
    if not 1 <= size <= limit or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ForestError("STORAGE_PROFILE_INPUT_INVALID", f"Invalid {field_name}", 422)
    return value


@dataclass(frozen=True)
class StorageProfileMetadata:
    profile_ref: str
    bot_id: str
    storage_generation: int
    dsn_secret_ref: str


@dataclass(frozen=True)
class ResolvedBotStorageProfile:
    profile_ref: str
    bot_id: str
    storage_generation: int
    dsn: str = field(repr=False)


class DeploymentStorageProfileAuthority:
    """Bind one opaque PostgreSQL credential reference to one Bot storage generation."""

    def __init__(self, store, *, secrets) -> None:
        self.store = store
        self.secrets = secrets

    async def create(
        self,
        *,
        operation_id: str,
        profile_ref: str,
        bot_id: str,
        storage_generation: int,
        dsn_secret_ref: str,
        authorization=None,
        conn=None,
        pinned_request_id: str | None = None,
    ) -> StorageProfileMetadata:
        _bounded(operation_id, "operation_id", 256)
        _bounded(bot_id, "bot_id", 128)
        if not isinstance(profile_ref, str) or _PROFILE_REF.fullmatch(profile_ref) is None:
            raise ForestError("STORAGE_PROFILE_INPUT_INVALID", "Profile reference is invalid", 422)
        if type(storage_generation) is not int or storage_generation < 1:
            raise ForestError("STORAGE_PROFILE_INPUT_INVALID", "Storage generation is invalid", 422)
        input_digest = digest_json(
            {
                "profile_ref": profile_ref,
                "bot_id": bot_id,
                "storage_generation": storage_generation,
                "dsn_secret_ref": dsn_secret_ref,
            }
        )
        async with self.store.transaction(conn) as conn:
            await authorize_control_action_in(
                conn, authorization, operation_id=operation_id
            )
            for key in (
                f"forest.deployment.storage-request/bot/{bot_id}/{storage_generation}",
                f"forest.deployment.storage-request/profile/{profile_ref}",
                f"forest.deployment.storage-request/dsn/{dsn_secret_ref}",
            ):
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (key,),
                )
            replay = await (
                await conn.execute(
                    "SELECT o.input_digest,p.* FROM forest_deployment.storage_profile_operations o "
                    "JOIN forest_deployment.storage_profiles p USING(profile_ref) "
                    "WHERE o.operation_id=%s",
                    (operation_id,),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "STORAGE_PROFILE_OPERATION_CONFLICT",
                        "Storage profile operation differs",
                        409,
                    )
                return self._metadata(replay)
            pending = await (
                await conn.execute(
                    "SELECT request_id,bot_id,storage_generation,profile_ref,"
                    "dsn_secret_ref FROM forest_deployment.storage_requests "
                    "WHERE deployment_id=%s AND ((bot_id=%s AND storage_generation=%s) "
                    "OR profile_ref=%s OR dsn_secret_ref=%s)",
                    (self.store.deployment_id, bot_id, storage_generation,
                     profile_ref, dsn_secret_ref),
                )
            ).fetchall()
            matching_request = (
                len(pending) == 1
                and pending[0]["request_id"] == pinned_request_id
                and pending[0]["bot_id"] == bot_id
                and pending[0]["storage_generation"] == storage_generation
                and pending[0]["profile_ref"] == profile_ref
                and pending[0]["dsn_secret_ref"] == dsn_secret_ref
            )
            if pending and not matching_request:
                raise ForestError(
                    "STORAGE_PROFILE_REQUEST_PENDING",
                    "Host storage request already owns this identity", 409,
                )
            if pinned_request_id is not None:
                pinned = None
                if matching_request:
                    pinned = await (
                        await conn.execute(
                            "SELECT 1 FROM forest_deployment.storage_request_pins "
                            "WHERE request_id=%s FOR SHARE", (pinned_request_id,),
                        )
                    ).fetchone()
                if pinned is None:
                    raise ForestError(
                        "STORAGE_REQUEST_NOT_PINNED",
                        "Host storage request has no pinned resource", 409,
                    )
            secret = await self.secrets.metadata_in(conn, dsn_secret_ref)
            if secret.purpose != "storage.postgresql.dsn":
                raise ForestError(
                    "STORAGE_PROFILE_SECRET_PURPOSE_INVALID",
                    "Storage profile requires a PostgreSQL DSN secret",
                    409,
                )
            existing = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.storage_profiles "
                    "WHERE profile_ref=%s OR (bot_id=%s AND storage_generation=%s) "
                    "OR dsn_secret_ref=%s LIMIT 1",
                    (profile_ref, bot_id, storage_generation, dsn_secret_ref),
                )
            ).fetchone()
            if existing is not None:
                raise ForestError(
                    "STORAGE_PROFILE_EXISTS", "Storage profile reference cannot be reused", 409
                )
            saved = await (
                await conn.execute(
                    "INSERT INTO forest_deployment.storage_profiles("
                    "profile_ref,bot_id,storage_generation,dsn_secret_ref) "
                    "VALUES (%s,%s,%s,%s) RETURNING *",
                    (profile_ref, bot_id, storage_generation, dsn_secret_ref),
                )
            ).fetchone()
            await conn.execute(
                "INSERT INTO forest_deployment.storage_profile_operations("
                "operation_id,input_digest,profile_ref) VALUES (%s,%s,%s)",
                (operation_id, input_digest, profile_ref),
            )
            return self._metadata(saved)

    async def metadata(self, profile_ref: str) -> StorageProfileMetadata:
        return self._metadata(await self._row(profile_ref))

    async def revoke(
        self, lease, *, operation_id: str, profile_ref: str,
        bot_id: str, expected_bot_revision: int,
    ) -> dict:
        """Freeze one Host-backed profile after the Bot runtime has drained."""
        _bounded(operation_id, "operation_id", 256)
        _bounded(bot_id, "bot_id", 128)
        if not isinstance(profile_ref, str) or _PROFILE_REF.fullmatch(profile_ref) is None:
            raise ForestError("STORAGE_PROFILE_INPUT_INVALID", "Profile reference is invalid", 422)
        if (
            type(expected_bot_revision) is not int
            or not 1 <= expected_bot_revision <= 9223372036854775807
        ):
            raise ForestError("STORAGE_PROFILE_INPUT_INVALID", "Bot revision is invalid", 422)
        input_digest = digest_json({
            "profile_ref": profile_ref, "bot_id": bot_id,
            "expected_bot_revision": expected_bot_revision,
        })
        async with self.store.pool.connection() as conn, conn.transaction():
            await verify_deployment_supervisor_in(
                conn, lease, self.store.deployment_id,
            )
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("forest.deployment.storage-profile-revoke/" + operation_id,),
            )
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("forest.deployment.storage-profile-revoke/profile/" + profile_ref,),
            )
            replay = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.storage_profile_revocations "
                    "WHERE operation_id=%s FOR SHARE", (operation_id,),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "STORAGE_PROFILE_REVOCATION_CONFLICT",
                        "Revocation operation differs", 409,
                    )
                return dict(replay)
            witness = await DeploymentCatalogRepository(
                self.store
            ).retirement_quiescence_in(conn, bot_id)
            if (
                witness["bot_revision"] != expected_bot_revision
                or witness["storage_profile_ref"] != profile_ref
            ):
                raise ForestError(
                    "STORAGE_PROFILE_RETIREMENT_MISMATCH",
                    "Profile differs from the fenced Bot retirement", 409,
                )
            pinned = await (
                await conn.execute(
                    "SELECT r.request_id,r.deployment_id,r.host_id AS request_host,"
                    "p.host_id AS pin_host,p.resource_id "
                    "FROM forest_deployment.storage_requests r "
                    "JOIN forest_deployment.storage_request_pins p "
                    "ON p.request_id=r.request_id AND p.profile_ref=r.profile_ref "
                    "WHERE r.profile_ref=%s AND r.bot_id=%s "
                    "AND r.storage_generation=%s FOR SHARE OF r,p",
                    (profile_ref, bot_id, witness["storage_generation"]),
                )
            ).fetchone()
            if pinned is None or (
                pinned["deployment_id"] != self.store.deployment_id
                or pinned["request_host"] != pinned["pin_host"]
            ):
                raise ForestError(
                    "STORAGE_PROFILE_NOT_HOST_OWNED",
                    "Profile has no exact Host pin", 409,
                )
            active_job = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.provisioning_jobs "
                    "WHERE bot_id=%s AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED') "
                    "FOR SHARE", (bot_id,),
                )
            ).fetchone()
            if active_job is not None:
                raise ForestError(
                    "STORAGE_PROFILE_REFERENCED",
                    "Bot provisioning still references the profile", 409,
                )
            existing = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.storage_profile_revocations "
                    "WHERE profile_ref=%s FOR SHARE", (profile_ref,),
                )
            ).fetchone()
            if existing is not None:
                raise ForestError(
                    "STORAGE_PROFILE_REVOCATION_CONFLICT",
                    "Profile already has another revocation", 409,
                )
            saved = await (
                await conn.execute(
                    "INSERT INTO forest_deployment.storage_profile_revocations("
                    "profile_ref,operation_id,input_digest,retirement_operation_id,"
                    "bot_id,storage_generation,request_id,resource_id,host_id,"
                    "bot_revision,intent_revision,observation_revision) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        profile_ref, operation_id, input_digest,
                        witness["retirement_operation_id"], bot_id,
                        witness["storage_generation"], pinned["request_id"],
                        pinned["resource_id"], pinned["pin_host"],
                        witness["bot_revision"], witness["intent_revision"],
                        witness["observation_revision"],
                    ),
                )
            ).fetchone()
            return dict(saved)

    async def resolve(
        self,
        *,
        profile_ref: str,
        expected_bot_id: str,
        expected_storage_generation: int,
    ) -> ResolvedBotStorageProfile:
        row = await self._row(profile_ref)
        if (
            row["bot_id"] != expected_bot_id
            or row["storage_generation"] != expected_storage_generation
        ):
            raise ForestError(
                "STORAGE_PROFILE_SCOPE_MISMATCH",
                "Storage profile belongs to another Bot or generation",
                403,
            )
        if row["revocation_operation_id"] is not None:
            raise ForestError(
                "STORAGE_PROFILE_REVOKED", "Storage profile is retired", 409,
            )
        dsn = await self.secrets.resolve_text(row["dsn_secret_ref"])
        try:
            parsed = urlsplit(dsn)
            valid = (
                parsed.scheme in {"postgres", "postgresql"}
                and parsed.hostname is not None
                and parsed.username is not None
                and parsed.path not in {"", "/"}
                and parsed.fragment == ""
            )
        except ValueError:
            valid = False
        if not valid:
            raise ForestError("STORAGE_PROFILE_DSN_INVALID", "Storage DSN is invalid", 422)
        return ResolvedBotStorageProfile(
            row["profile_ref"], row["bot_id"], row["storage_generation"], dsn
        )

    async def _row(self, profile_ref: str):
        if not isinstance(profile_ref, str) or _PROFILE_REF.fullmatch(profile_ref) is None:
            raise ForestError("STORAGE_PROFILE_INPUT_INVALID", "Profile reference is invalid", 422)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT p.*,r.operation_id AS revocation_operation_id "
                    "FROM forest_deployment.storage_profiles p "
                    "LEFT JOIN forest_deployment.storage_profile_revocations r "
                    "USING(profile_ref) WHERE p.profile_ref=%s",
                    (profile_ref,),
                )
            ).fetchone()
        if row is None:
            raise ForestError("STORAGE_PROFILE_NOT_FOUND", "Storage profile does not exist", 404)
        return row

    @staticmethod
    def _metadata(row) -> StorageProfileMetadata:
        return StorageProfileMetadata(
            row["profile_ref"],
            row["bot_id"],
            row["storage_generation"],
            row["dsn_secret_ref"],
        )

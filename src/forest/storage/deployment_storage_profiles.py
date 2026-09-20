"""Immutable deployment storage profiles resolved through the Secret authority."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from forest.errors import ForestError
from forest.packages import digest_json

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
    ) -> StorageProfileMetadata:
        _bounded(operation_id, "operation_id", 256)
        _bounded(bot_id, "bot_id", 128)
        if not isinstance(profile_ref, str) or _PROFILE_REF.fullmatch(profile_ref) is None:
            raise ForestError("STORAGE_PROFILE_INPUT_INVALID", "Profile reference is invalid", 422)
        if type(storage_generation) is not int or storage_generation < 1:
            raise ForestError("STORAGE_PROFILE_INPUT_INVALID", "Storage generation is invalid", 422)
        secret = await self.secrets.metadata(dsn_secret_ref)
        if secret.purpose != "storage.postgresql.dsn":
            raise ForestError(
                "STORAGE_PROFILE_SECRET_PURPOSE_INVALID",
                "Storage profile requires a PostgreSQL DSN secret",
                409,
            )
        input_digest = digest_json(
            {
                "profile_ref": profile_ref,
                "bot_id": bot_id,
                "storage_generation": storage_generation,
                "dsn_secret_ref": dsn_secret_ref,
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"forest.deployment.storage-profile/{profile_ref}",),
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
            existing = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.storage_profiles WHERE profile_ref=%s",
                    (profile_ref,),
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
                    "SELECT * FROM forest_deployment.storage_profiles WHERE profile_ref=%s",
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

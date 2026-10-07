"""Immutable deployment intent for Host-owned Bot storage generations."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from forest.deployment_action_authorization import authorize_deployment_action_in
from forest.errors import ForestError
from forest.packages import digest_json

_HOST_ID = re.compile(r"host/[a-f0-9]{32}\Z")
_PROFILE_REF = re.compile(r"storage-profile/[A-Za-z0-9][A-Za-z0-9._/-]{0,495}\Z")
_SECRET_REF = re.compile(r"secret://[A-Za-z0-9][A-Za-z0-9._/-]{0,501}\Z")


def _bounded(value: object, field: str, limit: int) -> str:
    if not isinstance(value, str):
        raise ForestError("STORAGE_REQUEST_INVALID", f"Invalid {field}", 422)
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError:
        size = 0
    if not 1 <= size <= limit or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ForestError("STORAGE_REQUEST_INVALID", f"Invalid {field}", 422)
    return value


@dataclass(frozen=True)
class StorageRequest:
    request_id: str
    deployment_id: str
    host_id: str
    bot_id: str
    storage_generation: int
    profile_ref: str
    dsn_secret_ref: str
    credential_ref: str
    state: str = "REQUESTED"


class DeploymentStorageRequestAuthority:
    """Coalesce concurrent intents before any Host call or usable profile exists."""

    def __init__(self, store) -> None:
        self.store = store

    async def request(
        self, *, operation_id: str, host_id: str, bot_id: str,
        storage_generation: int, profile_ref: str, dsn_secret_ref: str,
        credential_ref: str, authorization=None, conn=None,
    ) -> StorageRequest:
        _bounded(operation_id, "operation_id", 256)
        _bounded(bot_id, "bot_id", 128)
        _bounded(credential_ref, "credential_ref", 512)
        if (
            not isinstance(host_id, str) or _HOST_ID.fullmatch(host_id) is None
            or not isinstance(profile_ref, str)
            or _PROFILE_REF.fullmatch(profile_ref) is None
            or not isinstance(dsn_secret_ref, str)
            or _SECRET_REF.fullmatch(dsn_secret_ref) is None
            or credential_ref == dsn_secret_ref
            or len(credential_ref.encode("utf-8")) < 10
            or type(storage_generation) is not int
            or not 1 <= storage_generation <= 9223372036854775807
        ):
            raise ForestError("STORAGE_REQUEST_INVALID", "Storage request is invalid", 422)
        deployment_id = self.store.deployment_id
        request_id = "storage-request/" + hashlib.sha256(
            f"{deployment_id}\0{bot_id}\0{storage_generation}".encode("utf-8")
        ).hexdigest()
        input_digest = digest_json({
            "deployment_id": deployment_id,
            "host_id": host_id,
            "bot_id": bot_id,
            "storage_generation": storage_generation,
            "profile_ref": profile_ref,
            "dsn_secret_ref": dsn_secret_ref,
            "credential_ref": credential_ref,
        })
        async with self.store.transaction(conn) as conn:
            await authorize_deployment_action_in(
                conn, authorization, operation_id=operation_id,
            )
            for key in (
                f"forest.deployment.storage-request/bot/{bot_id}/{storage_generation}",
                f"forest.deployment.storage-request/profile/{profile_ref}",
                f"forest.deployment.storage-request/dsn/{dsn_secret_ref}",
            ):
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (key,),
                )
            existing = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.storage_requests "
                    "WHERE request_id=%s FOR SHARE", (request_id,),
                )
            ).fetchone()
            if existing is not None:
                if existing["input_digest"] != input_digest:
                    raise ForestError(
                        "STORAGE_REQUEST_CONFLICT",
                        "Storage generation already has a different request", 409,
                    )
                return self._request(existing)
            conflict = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.storage_requests "
                    "WHERE deployment_id=%s AND (profile_ref=%s OR dsn_secret_ref=%s) "
                    "UNION ALL SELECT 1 FROM forest_deployment.storage_profiles "
                    "WHERE (bot_id=%s AND storage_generation=%s) "
                    "OR profile_ref=%s OR dsn_secret_ref=%s LIMIT 1",
                    (deployment_id, profile_ref, dsn_secret_ref,
                     bot_id, storage_generation, profile_ref, dsn_secret_ref),
                )
            ).fetchone()
            if conflict is not None:
                raise ForestError(
                    "STORAGE_REQUEST_CONFLICT",
                    "Storage identity is already allocated", 409,
                )
            occupied_secret = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.secret_slots "
                    "WHERE secret_ref=%s", (dsn_secret_ref,),
                )
            ).fetchone()
            if occupied_secret is not None:
                raise ForestError(
                    "STORAGE_REQUEST_CONFLICT",
                    "DSN Secret reference is already occupied", 409,
                )
            saved = await (
                await conn.execute(
                    "INSERT INTO forest_deployment.storage_requests("
                    "request_id,deployment_id,host_id,bot_id,storage_generation,"
                    "profile_ref,dsn_secret_ref,credential_ref,input_digest) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (request_id, deployment_id, host_id, bot_id,
                     storage_generation, profile_ref, dsn_secret_ref,
                     credential_ref, input_digest),
                )
            ).fetchone()
            return self._request(saved)

    async def read(self, request_id: str) -> StorageRequest:
        if not isinstance(request_id, str) or not re.fullmatch(
            r"storage-request/[a-f0-9]{64}", request_id,
        ):
            raise ForestError("STORAGE_REQUEST_INVALID", "Request ID is invalid", 422)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.storage_requests "
                    "WHERE deployment_id=%s AND request_id=%s",
                    (self.store.deployment_id, request_id),
                )
            ).fetchone()
        if row is None:
            raise ForestError("STORAGE_REQUEST_NOT_FOUND", "Storage request not found", 404)
        return self._request(row)

    @staticmethod
    def _request(row) -> StorageRequest:
        return StorageRequest(
            request_id=row["request_id"],
            deployment_id=row["deployment_id"],
            host_id=row["host_id"],
            bot_id=row["bot_id"],
            storage_generation=row["storage_generation"],
            profile_ref=row["profile_ref"],
            dsn_secret_ref=row["dsn_secret_ref"],
            credential_ref=row["credential_ref"],
        )

"""Atomically publish a pinned Host database to one deployment Bot profile."""

from __future__ import annotations

import hashlib
import re
from urllib.parse import quote, urlsplit, urlunsplit

from forest.errors import ForestError
from forest.storage.deployment_catalog import verify_deployment_supervisor_in
from forest.storage.deployment_storage_profiles import StorageProfileMetadata

_HOST_ID = re.compile(r"host/[a-f0-9]{32}\Z")
_REQUEST_ID = re.compile(r"storage-request/[a-f0-9]{64}\Z")


class DeploymentStorageSettlementAuthority:
    """Join Secret and profile authorities only after the exact Host pin exists."""

    def __init__(self, store, *, secrets, profiles, host_id: str,
                 maintenance_dsn: str) -> None:
        if not isinstance(host_id, str) or _HOST_ID.fullmatch(host_id) is None:
            raise ValueError("Host storage identity is invalid")
        try:
            parsed = urlsplit(maintenance_dsn)
            valid = (
                parsed.scheme in {"postgres", "postgresql"}
                and parsed.hostname is not None and parsed.username is not None
                and parsed.path not in {"", "/"} and parsed.fragment == ""
                and (parsed.port is None or 1 <= parsed.port <= 65535)
            )
        except (TypeError, ValueError):
            valid = False
        if not valid:
            raise ValueError("Host PostgreSQL maintenance endpoint is invalid")
        self.store = store
        self.secrets = secrets
        self.profiles = profiles
        self.host_id = host_id
        self._endpoint = parsed

    async def finalize(self, supervisor, *, request_id: str,
                       password: str) -> StorageProfileMetadata:
        if not isinstance(request_id, str) or _REQUEST_ID.fullmatch(request_id) is None:
            raise ForestError("STORAGE_REQUEST_INVALID", "Request ID is invalid", 422)
        if (
            not isinstance(password, str) or not 32 <= len(password) <= 256
            or not password.isascii()
            or any(not 33 <= ord(char) <= 126 for char in password)
        ):
            raise ForestError("STORAGE_CREDENTIAL_INVALID", "Role password is invalid", 422)
        credential_digest = "sha256:" + hashlib.sha256(password.encode()).hexdigest()
        async with self.store.transaction() as conn:
            await verify_deployment_supervisor_in(
                conn, supervisor, self.store.deployment_id,
            )
            row = await (
                await conn.execute(
                    "SELECT r.*,p.resource_id,p.host_id AS pinned_host_id,"
                    "p.credential_digest AS pin_credential_digest,"
                    "p.physical_name,p.role_name "
                    "FROM forest_deployment.storage_requests r "
                    "JOIN forest_deployment.storage_request_pins p USING(request_id) "
                    "WHERE r.deployment_id=%s AND r.request_id=%s FOR SHARE OF r,p",
                    (self.store.deployment_id, request_id),
                )
            ).fetchone()
            if row is None:
                raise ForestError(
                    "STORAGE_REQUEST_NOT_PINNED", "Host storage request is not pinned", 409,
                )
            if (
                row["host_id"] != self.host_id
                or row["pinned_host_id"] != self.host_id
                or row["credential_digest"] != credential_digest
                or row["pin_credential_digest"] != credential_digest
            ):
                raise ForestError(
                    "STORAGE_SETTLEMENT_MISMATCH",
                    "Host pin and role credential do not match", 409,
                )
            frozen_secret = await self.secrets.metadata_in(conn, row["credential_ref"])
            if (
                frozen_secret.purpose != "storage.postgresql.password"
                or frozen_secret.version != row["credential_version"]
            ):
                raise ForestError(
                    "STORAGE_SETTLEMENT_MISMATCH",
                    "Frozen role credential version differs", 409,
                )
            dsn = self._target_dsn(row["role_name"], row["physical_name"], password)
            dsn_bytes = dsn.encode("utf-8")
            for key in (
                f"forest.deployment.storage-request/bot/"
                    f"{row['bot_id']}/{row['storage_generation']}",
                f"forest.deployment.storage-request/profile/{row['profile_ref']}",
                f"forest.deployment.storage-request/dsn/{row['dsn_secret_ref']}",
                f"forest.deployment.secret-operation/{request_id}/dsn",
                f"forest.deployment.storage-settlement/request/{request_id}",
            ):
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (key,),
                )
            existing = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.storage_profiles "
                    "WHERE profile_ref=%s OR (bot_id=%s AND storage_generation=%s) "
                    "OR dsn_secret_ref=%s FOR SHARE",
                    (row["profile_ref"], row["bot_id"],
                     row["storage_generation"], row["dsn_secret_ref"]),
                )
            ).fetchall()
            if existing:
                if len(existing) != 1 or any(
                    existing[0][key] != row[key]
                    for key in ("profile_ref", "bot_id", "storage_generation",
                                "dsn_secret_ref")
                ):
                    raise ForestError(
                        "STORAGE_SETTLEMENT_CONFLICT", "Storage profile differs", 409,
                    )
                metadata = await self.secrets.metadata_in(conn, row["dsn_secret_ref"])
                if metadata.purpose != "storage.postgresql.dsn":
                    raise ForestError(
                        "STORAGE_SETTLEMENT_CONFLICT", "Storage Secret purpose differs", 409,
                    )
                current_dsn = await self.secrets.resolve_in(conn, row["dsn_secret_ref"])
                if current_dsn != dsn_bytes:
                    raise ForestError(
                        "STORAGE_SETTLEMENT_CONFLICT", "Storage DSN differs", 409,
                    )
                return StorageProfileMetadata(
                    existing[0]["profile_ref"], existing[0]["bot_id"],
                    existing[0]["storage_generation"],
                    existing[0]["dsn_secret_ref"],
                )
            await self.secrets.put_in(
                conn, operation_id=request_id + "/dsn",
                secret_ref=row["dsn_secret_ref"],
                purpose="storage.postgresql.dsn", plaintext=dsn_bytes,
                expected_revision=0, pinned_request_id=request_id,
            )
            return await self.profiles.create(
                operation_id=request_id + "/profile", conn=conn,
                profile_ref=row["profile_ref"], bot_id=row["bot_id"],
                storage_generation=row["storage_generation"],
                dsn_secret_ref=row["dsn_secret_ref"],
                pinned_request_id=request_id,
            )

    def _target_dsn(self, role_name: str, physical_name: str, password: str) -> str:
        host = self._endpoint.hostname
        if ":" in host:
            host = f"[{host}]"
        port = "" if self._endpoint.port is None else f":{self._endpoint.port}"
        netloc = (
            f"{quote(role_name, safe='')}:{quote(password, safe='')}@{host}{port}"
        )
        return urlunsplit(self._endpoint._replace(
            netloc=netloc, path="/" + physical_name,
        ))

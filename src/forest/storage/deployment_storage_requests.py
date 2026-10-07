"""Immutable deployment intent for Host-owned Bot storage generations."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from forest.deployment_action_authorization import authorize_deployment_action_in
from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.deployment_catalog import (
    DeploymentSupervisorLease,
    verify_deployment_supervisor_in,
)

_HOST_ID = re.compile(r"host/[a-f0-9]{32}\Z")
_RESOURCE_ID = re.compile(r"resource/[a-f0-9-]{36}\Z")
_REQUEST_ID = re.compile(r"storage-request/[a-f0-9]{64}\Z")
_PROFILE_REF = re.compile(r"storage-profile/[A-Za-z0-9][A-Za-z0-9._/-]{0,495}\Z")
_SECRET_REF = re.compile(r"secret://[A-Za-z0-9][A-Za-z0-9._/-]{0,501}\Z")
_PHYSICAL_NAME = re.compile(r"forest_bot_[a-f0-9]{24}\Z")
_ROLE_NAME = re.compile(r"forest_role_[a-f0-9]{24}\Z")


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


@dataclass(frozen=True)
class StorageResourceBinding:
    request_id: str
    resource_id: str


@dataclass(frozen=True)
class StoragePinnedResource:
    request_id: str
    resource_id: str
    host_id: str
    resource_revision: int
    physical_name: str
    role_name: str


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
        if not isinstance(request_id, str) or _REQUEST_ID.fullmatch(request_id) is None:
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

    async def bind_resource(
        self, supervisor: DeploymentSupervisorLease, *, request_id: str,
        receipt: dict, conn=None,
    ) -> StorageResourceBinding:
        """Persist only the immutable Host resource identity from a trusted bridge."""
        expected_keys = {
            "resource_id", "host_id", "deployment_id", "bot_id",
            "storage_generation", "state", "revision",
        }
        if (
            not isinstance(request_id, str) or _REQUEST_ID.fullmatch(request_id) is None
            or type(receipt) is not dict or set(receipt) != expected_keys
            or not isinstance(receipt["resource_id"], str)
            or _RESOURCE_ID.fullmatch(receipt["resource_id"]) is None
            or type(receipt["state"]) is not str
            or receipt["state"] not in {"RESERVED", "ACTIVE"}
            or type(receipt["revision"]) is not int
            or receipt["revision"] < 1
            or type(receipt["storage_generation"]) is not int
        ):
            raise ForestError("STORAGE_RESOURCE_RECEIPT_INVALID", "Host receipt is invalid", 422)
        resource_id = receipt["resource_id"]
        async with self.store.transaction(conn) as conn:
            await verify_deployment_supervisor_in(
                conn, supervisor, self.store.deployment_id,
            )
            for key in (
                f"forest.deployment.storage-binding/request/{request_id}",
                f"forest.deployment.storage-binding/resource/{resource_id}",
            ):
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (key,),
                )
            request = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.storage_requests "
                    "WHERE request_id=%s AND deployment_id=%s FOR SHARE",
                    (request_id, self.store.deployment_id),
                )
            ).fetchone()
            if request is None:
                raise ForestError(
                    "STORAGE_REQUEST_NOT_FOUND", "Storage request not found", 404,
                )
            if (
                receipt["deployment_id"] != request["deployment_id"]
                or receipt["host_id"] != request["host_id"]
                or receipt["bot_id"] != request["bot_id"]
                or receipt["storage_generation"] != request["storage_generation"]
            ):
                raise ForestError(
                    "STORAGE_RESOURCE_MISMATCH", "Host resource belongs to another request", 409,
                )
            binding = await (
                await conn.execute(
                    "SELECT request_id,resource_id FROM "
                    "forest_deployment.storage_request_resources "
                    "WHERE request_id=%s OR resource_id=%s FOR SHARE",
                    (request_id, resource_id),
                )
            ).fetchone()
            if binding is not None:
                if binding["request_id"] != request_id or binding["resource_id"] != resource_id:
                    raise ForestError(
                        "STORAGE_RESOURCE_CONFLICT",
                        "Request or Host resource is already bound differently", 409,
                    )
                return StorageResourceBinding(request_id, resource_id)
            await conn.execute(
                "INSERT INTO forest_deployment.storage_request_resources("
                "request_id,resource_id) VALUES (%s,%s)",
                (request_id, resource_id),
            )
            return StorageResourceBinding(request_id, resource_id)

    async def resource_binding(self, request_id: str) -> StorageResourceBinding | None:
        if not isinstance(request_id, str) or _REQUEST_ID.fullmatch(request_id) is None:
            raise ForestError("STORAGE_REQUEST_INVALID", "Request ID is invalid", 422)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT b.request_id,b.resource_id FROM "
                    "forest_deployment.storage_request_resources b "
                    "JOIN forest_deployment.storage_requests r USING(request_id) "
                    "WHERE r.deployment_id=%s AND b.request_id=%s",
                    (self.store.deployment_id, request_id),
                )
            ).fetchone()
        return None if row is None else StorageResourceBinding(**row)

    async def bind_pin(
        self, supervisor: DeploymentSupervisorLease, *, request_id: str,
        pin_receipt: dict, resource: dict, conn=None,
    ) -> StoragePinnedResource:
        """Record one Host pin and the exact physical identity it protects."""
        pin_keys = {
            "resource_id", "host_id", "deployment_id", "bot_id",
            "storage_generation", "profile_ref", "state", "resource_revision",
        }
        resource_keys = {
            "resource_id", "host_id", "deployment_id", "bot_id",
            "storage_generation", "kind", "physical_name", "role_name",
            "state", "revision",
        }
        if (
            not isinstance(request_id, str) or _REQUEST_ID.fullmatch(request_id) is None
            or type(pin_receipt) is not dict or set(pin_receipt) != pin_keys
            or type(resource) is not dict or set(resource) != resource_keys
            or not isinstance(pin_receipt["resource_id"], str)
            or _RESOURCE_ID.fullmatch(pin_receipt["resource_id"]) is None
            or not isinstance(pin_receipt["host_id"], str)
            or _HOST_ID.fullmatch(pin_receipt["host_id"]) is None
            or type(pin_receipt["resource_revision"]) is not int
            or pin_receipt["resource_revision"] < 2
            or pin_receipt["state"] != "PINNED"
            or resource["state"] != "ACTIVE"
            or resource["kind"] != "POSTGRES_DATABASE"
            or type(resource["revision"]) is not int
            or resource["revision"] != pin_receipt["resource_revision"]
            or not isinstance(resource["physical_name"], str)
            or _PHYSICAL_NAME.fullmatch(resource["physical_name"]) is None
            or not isinstance(resource["role_name"], str)
            or _ROLE_NAME.fullmatch(resource["role_name"]) is None
            or any(resource[key] != pin_receipt[key] for key in (
                "resource_id", "host_id", "deployment_id", "bot_id",
                "storage_generation",
            ))
        ):
            raise ForestError("STORAGE_PIN_RECEIPT_INVALID", "Host pin receipt is invalid", 422)
        async with self.store.transaction(conn) as conn:
            await verify_deployment_supervisor_in(
                conn, supervisor, self.store.deployment_id,
            )
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"forest.deployment.storage-pin/request/{request_id}",),
            )
            request = await (
                await conn.execute(
                    "SELECT r.*,b.resource_id AS bound_resource_id "
                    "FROM forest_deployment.storage_requests r "
                    "JOIN forest_deployment.storage_request_resources b USING(request_id) "
                    "WHERE r.deployment_id=%s AND r.request_id=%s FOR SHARE OF r,b",
                    (self.store.deployment_id, request_id),
                )
            ).fetchone()
            if request is None:
                raise ForestError(
                    "STORAGE_RESOURCE_NOT_BOUND", "Host resource is not bound", 409,
                )
            if (
                request["bound_resource_id"] != pin_receipt["resource_id"]
                or request["deployment_id"] != pin_receipt["deployment_id"]
                or request["host_id"] != pin_receipt["host_id"]
                or request["bot_id"] != pin_receipt["bot_id"]
                or request["storage_generation"] != pin_receipt["storage_generation"]
                or request["profile_ref"] != pin_receipt["profile_ref"]
            ):
                raise ForestError(
                    "STORAGE_PIN_MISMATCH", "Host pin belongs to another request", 409,
                )
            replay = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.storage_request_pins "
                    "WHERE request_id=%s FOR SHARE", (request_id,),
                )
            ).fetchone()
            expected = (
                pin_receipt["resource_id"], pin_receipt["host_id"],
                pin_receipt["resource_revision"], resource["physical_name"],
                resource["role_name"],
            )
            if replay is not None:
                if self._pin_values(replay) != expected:
                    raise ForestError(
                        "STORAGE_PIN_CONFLICT", "Host pin receipt differs", 409,
                    )
                return self._pinned(replay)
            saved = await (
                await conn.execute(
                    "INSERT INTO forest_deployment.storage_request_pins("
                    "request_id,resource_id,deployment_id,host_id,profile_ref,"
                    "resource_revision,physical_name,role_name) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (request_id, expected[0], request["deployment_id"],
                     expected[1], request["profile_ref"], *expected[2:]),
                )
            ).fetchone()
            return self._pinned(saved)

    async def pinned_resource(self, request_id: str) -> StoragePinnedResource | None:
        if not isinstance(request_id, str) or _REQUEST_ID.fullmatch(request_id) is None:
            raise ForestError("STORAGE_REQUEST_INVALID", "Request ID is invalid", 422)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT p.* FROM forest_deployment.storage_request_pins p "
                    "JOIN forest_deployment.storage_requests r USING(request_id) "
                    "WHERE r.deployment_id=%s AND p.request_id=%s",
                    (self.store.deployment_id, request_id),
                )
            ).fetchone()
        return None if row is None else self._pinned(row)

    @staticmethod
    def _pin_values(row) -> tuple:
        return (
            row["resource_id"], row["host_id"], row["resource_revision"],
            row["physical_name"], row["role_name"],
        )

    @staticmethod
    def _pinned(row) -> StoragePinnedResource:
        return StoragePinnedResource(
            row["request_id"], *DeploymentStorageRequestAuthority._pin_values(row),
        )

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

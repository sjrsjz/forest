"""Exact Host-owned PostgreSQL creation with crash-recoverable physical markers."""

from __future__ import annotations

import hashlib
import re
from urllib.parse import urlsplit, urlunsplit

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from forest.errors import ForestError
from forest.schema_bundles import module_schema_namespace
from forest.storage.host_generation import HostGenerationAuthority
from forest.storage.host_identity import HostIdentityAuthority

_RESOURCE = re.compile(r"resource/[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")
_ROLE_FLAGS = {
    "rolsuper": False, "rolinherit": True, "rolcreaterole": False,
    "rolcreatedb": False, "rolcanlogin": True,
    "rolreplication": False, "rolbypassrls": False,
}
_NAMESPACE = module_schema_namespace("forest.host.storage")


class HostPostgresMaterializationEffect:
    """Native Effect bound to one host.storage Generation, never to caller SQL names."""

    def __init__(
        self, configuration, configuration_revision, identity, declaration, *,
        store, supervisor, maintenance_dsn: str, credential_resolver,
    ) -> None:
        if (
            identity.component_id != "host.storage"
            or declaration.get("name") != "host.storage.postgres.materialize"
            or declaration.get("kind") != "host.postgresql-materializer"
            or declaration.get("semantics") != "idempotent"
            or credential_resolver is None
        ):
            raise ForestError(
                "HOST_STORAGE_EFFECT_BINDING_INVALID",
                "Host PostgreSQL Effect has another binding", 500,
            )
        parsed = urlsplit(maintenance_dsn)
        if (
            parsed.scheme not in {"postgres", "postgresql"}
            or not parsed.hostname or not parsed.username or not parsed.path
        ):
            raise ValueError("Host PostgreSQL maintenance endpoint is invalid")
        self._maintenance_dsn = urlunsplit(parsed._replace(path="/postgres"))
        self._store = store
        self._supervisor = supervisor
        self._generations = HostGenerationAuthority(store)
        self._host_identity = HostIdentityAuthority(store)
        self._identity = identity
        self._credential_resolver = credential_resolver
        self._accepting = False

    async def start(self) -> None:
        self._accepting = True

    async def quiesce(self) -> None:
        self._accepting = False

    async def resume(self) -> None:
        self._accepting = True

    async def close(self) -> None:
        self._accepting = False

    async def _admitted(self) -> None:
        if not self._accepting:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE", "Host storage Effect is quiescing", 409,
            )
        await self._generations.verify_active_generation(
            component_id="host.storage",
            generation_id=self._identity.generation_id,
            admission_lane="DATA",
        )
        async with self._store.pool.connection() as conn, conn.transaction():
            await self._host_identity.verify_in(
                conn, self._supervisor.authority_lease(),
            )

    async def _reservation(self, resource_id: str) -> dict:
        if not isinstance(resource_id, str) or _RESOURCE.fullmatch(resource_id) is None:
            raise ForestError(
                "HOST_STORAGE_RESOURCE_INVALID", "Resource identity is invalid", 422,
            )
        async with self._store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {}.physical_generations WHERE resource_id=%s").format(
                        sql.Identifier(_NAMESPACE)
                    ), (resource_id,),
                )
            ).fetchone()
        if (
            row is None or row["host_id"] != self._store.host_id
            or row["kind"] != "POSTGRES_DATABASE"
            or row["state"] not in {"RESERVED", "ACTIVE"}
        ):
            raise ForestError(
                "HOST_STORAGE_RESOURCE_UNAVAILABLE",
                "Host PostgreSQL reservation is unavailable", 409,
            )
        return row

    async def invoke(self, payload: dict, *, scope, invocation) -> dict:
        if (
            scope.scope_kind != "host" or scope.scope_id != self._store.host_id
            or scope.component_id != "host.storage"
            or scope.generation_id != self._identity.generation_id
        ):
            raise ForestError(
                "HOST_STORAGE_EFFECT_BINDING_INVALID",
                "Host PostgreSQL Effect call has another Generation", 409,
            )
        await self._admitted()
        row = await self._reservation(payload["resource_id"])
        password = await self._credential_resolver(
            payload["credential_ref"], "storage.postgresql.password",
        )
        if (
            not isinstance(password, str) or not 32 <= len(password) <= 256
            or not password.isascii()
            or any(not 33 <= ord(char) <= 126 for char in password)
        ):
            raise ForestError(
                "HOST_STORAGE_CREDENTIAL_INVALID",
                "PostgreSQL role credential is invalid", 422,
            )
        marker = (
            "forest.host-storage/1|" + row["resource_id"] + "|"
            + row["host_id"] + "|" + row["owner_marker"]
        )
        await self._ensure_physical(row, marker, password)
        await self._admitted()
        return {
            "protocol": "forest.host-storage-materialization/1",
            "resource_id": row["resource_id"],
            "host_id": row["host_id"],
            "physical_name": row["physical_name"],
            "role_name": row["role_name"],
            "owner_marker_digest": "sha256:" + hashlib.sha256(marker.encode()).hexdigest(),
            "credential_digest": "sha256:" + hashlib.sha256(password.encode()).hexdigest(),
        }

    async def _ensure_physical(self, row: dict, marker: str, password: str) -> None:
        name, role = row["physical_name"], row["role_name"]
        try:
            conn = await psycopg.AsyncConnection.connect(
                self._maintenance_dsn, autocommit=True, row_factory=dict_row,
            )
            try:
                await conn.execute(
                    "SELECT pg_advisory_lock(hashtextextended(%s,0))",
                    ("forest.host-storage.postgresql/" + name,),
                )
                async with conn.transaction():
                    saved_role = await (
                        await conn.execute(
                            "SELECT rolsuper,rolinherit,rolcreaterole,rolcreatedb,"
                            "rolcanlogin,rolreplication,rolbypassrls,"
                            "shobj_description(oid,'pg_authid') AS marker, "
                            "EXISTS (SELECT 1 FROM pg_auth_members "
                            "WHERE roleid=pg_authid.oid OR member=pg_authid.oid) "
                            "AS has_members "
                            "FROM pg_authid WHERE rolname=%s", (role,),
                        )
                    ).fetchone()
                    if saved_role is None:
                        await conn.execute(sql.SQL(
                            "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER INHERIT "
                            "NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
                        ).format(sql.Identifier(role), sql.Literal(password)))
                        await conn.execute(sql.SQL("COMMENT ON ROLE {} IS {}").format(
                            sql.Identifier(role), sql.Literal(marker),
                        ))
                    elif (
                        saved_role["marker"] != marker
                        or saved_role["has_members"]
                        or any(saved_role[key] != value for key, value in _ROLE_FLAGS.items())
                    ):
                        raise ForestError(
                            "HOST_STORAGE_ROLE_MISMATCH",
                            "Physical PostgreSQL role has another owner or privileges", 409,
                        )
                    else:
                        await conn.execute(sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                            sql.Identifier(role), sql.Literal(password),
                        ))
                database = await self._database(conn, name)
                if database is None:
                    await conn.execute(sql.SQL(
                        "CREATE DATABASE {} OWNER {} CONNECTION LIMIT 0"
                    ).format(
                        sql.Identifier(name), sql.Identifier(role),
                    ))
                    database = await self._database(conn, name)
                if database is None or database["owner"] != role or (
                    database["marker"] is not None and database["marker"] != marker
                ) or (database["marker"] is None and row["state"] != "RESERVED"):
                    raise ForestError(
                        "HOST_STORAGE_DATABASE_MISMATCH",
                        "Physical PostgreSQL database has another owner or marker", 409,
                    )
                if database["foreign_grants"] or (
                    database["marker"] is not None and database["public_grants"]
                ) or database["connection_limit"] not in {0, -1} or (
                    database["connection_limit"] == 0 and row["state"] != "RESERVED"
                ):
                    raise ForestError(
                        "HOST_STORAGE_DATABASE_MISMATCH",
                        "Physical PostgreSQL database access differs from its reservation", 409,
                    )
                if database["marker"] is None:
                    if database["connection_limit"] != 0:
                        raise ForestError(
                            "HOST_STORAGE_DATABASE_MISMATCH",
                            "Unmarked PostgreSQL database is not admission-fenced", 409,
                        )
                    await conn.execute(sql.SQL(
                        "REVOKE ALL PRIVILEGES ON DATABASE {} FROM PUBLIC"
                    ).format(sql.Identifier(name)))
                    await conn.execute(sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                        sql.Identifier(name), sql.Literal(marker),
                    ))
                if database["connection_limit"] == 0:
                    await conn.execute(sql.SQL(
                        "ALTER DATABASE {} CONNECTION LIMIT -1"
                    ).format(sql.Identifier(name)))
                confirmed = await self._database(conn, name)
                if (
                    confirmed is None or confirmed["owner"] != role
                    or confirmed["marker"] != marker
                    or confirmed["public_grants"] or confirmed["foreign_grants"]
                    or confirmed["connection_limit"] != -1
                ):
                    raise ForestError(
                        "HOST_STORAGE_DATABASE_MISMATCH",
                        "Physical PostgreSQL marker could not be confirmed", 409,
                    )
            finally:
                await conn.close()
        except ForestError:
            raise
        except psycopg.Error:
            raise ForestError(
                "HOST_STORAGE_PHYSICAL_UNKNOWN",
                "PostgreSQL creation outcome requires exact-marker reconciliation", 503,
            ) from None

    @staticmethod
    async def _database(conn, name: str) -> dict | None:
        return await (
            await conn.execute(
                "SELECT pg_get_userbyid(datdba) AS owner,"
                "shobj_description(oid,'pg_database') AS marker, "
                "datconnlimit AS connection_limit, "
                "EXISTS (SELECT 1 FROM aclexplode(COALESCE(datacl, "
                "acldefault('d',datdba))) AS grant_row "
                "WHERE grant_row.grantee=0) AS public_grants, "
                "EXISTS (SELECT 1 FROM aclexplode(COALESCE(datacl, "
                "acldefault('d',datdba))) AS grant_row "
                "WHERE grant_row.grantee<>0 AND grant_row.grantee<>datdba) "
                "AS foreign_grants "
                "FROM pg_database WHERE datname=%s", (name,),
            )
        ).fetchone()

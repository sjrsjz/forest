"""Scope-fenced SQL bundle installation for control-plane package databases."""

from __future__ import annotations

import hashlib
import re

import psycopg
from psycopg import sql

from forest.errors import ForestError
from forest.schema_bundles import (
    StorageBundleSpec,
    validate_official_storage_baseline,
    validate_storage_baseline,
)

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_MODULE_SCHEMA_TOKEN = "__forest_module_schema__"
_SCOPE = {
    "deployment": ("forest_deployment", "deployment_identity", "deployment_id"),
    "host": ("forest_host", "host_identity", "host_id"),
}


class ControlScopeSchemaBundleInstaller:
    """Install a package namespace only into its exact control scope."""

    scope: str

    def __init__(self, store) -> None:
        self.store = store
        if self.scope not in _SCOPE:
            raise ValueError("Control package storage scope is invalid")

    async def install(
        self,
        spec: StorageBundleSpec,
        baseline: bytes,
        package_digest: str,
        *,
        execution_profile: str = "declarative",
    ) -> dict:
        scope = self.scope
        schema, identity_table, identity_column = _SCOPE[scope]
        scope_id = getattr(self.store, identity_column)
        if spec.scope != scope:
            raise ForestError(
                "INVALID_STORAGE_BUNDLE", f"{scope} storage requires {scope} scope", 422,
            )
        if execution_profile == "declarative":
            source = validate_storage_baseline(baseline, spec.baseline_sha256)
        elif execution_profile == "official":
            source = validate_official_storage_baseline(baseline, spec.baseline_sha256)
        else:
            raise ForestError("INVALID_STORAGE_BUNDLE", "Storage execution profile is invalid", 422)
        if not isinstance(package_digest, str) or _DIGEST.fullmatch(package_digest) is None:
            raise ForestError("INVALID_STORAGE_BUNDLE", "Storage package digest is invalid", 422)

        try:
            async with self.store.pool.connection() as conn, conn.transaction():
                await conn.execute("SET LOCAL lock_timeout='5s'")
                lock = hashlib.sha256(
                    f"forest.{scope}.schema-bundle/1\0".encode()
                    + scope_id.encode() + b"\0" + spec.schema_id.encode()
                ).digest()
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (int.from_bytes(lock[:8], "big", signed=True),),
                )
                identity = await (await conn.execute(
                    f"SELECT {identity_column} FROM {schema}.{identity_table} "
                    "WHERE singleton FOR SHARE"
                )).fetchone()
                if identity is None or identity[identity_column] != scope_id:
                    raise ForestError(
                        f"{scope.upper()}_SCOPE_MISMATCH",
                        f"Schema authority belongs to another {scope}", 403,
                    )
                existing = await (await conn.execute(
                    f"SELECT * FROM {schema}.schema_bundles "
                    f"WHERE {identity_column}=%s AND schema_id=%s FOR UPDATE",
                    (scope_id, spec.schema_id),
                )).fetchone()
                expected = {
                    "component_id": spec.component_id,
                    "schema_version": spec.schema_version,
                    "scope": scope,
                    "namespace": spec.namespace,
                    "baseline_sha256": spec.baseline_sha256,
                    "package_digest": package_digest,
                    "repository_contract": spec.repository_contract,
                    "execution_profile": execution_profile,
                    "state": "READY",
                }
                if existing is not None:
                    if all(str(existing[key]) == value for key, value in expected.items()):
                        return dict(existing)
                    raise ForestError(
                        "SCHEMA_BUNDLE_MISMATCH",
                        "Installed schema identity does not match the requested bundle", 409,
                    )
                occupied = await (await conn.execute(
                    "SELECT 1 FROM pg_namespace WHERE nspname=%s", (spec.namespace,)
                )).fetchone()
                if occupied is not None:
                    raise ForestError(
                        "SCHEMA_NAMESPACE_OCCUPIED",
                        "Module namespace exists without a matching ledger entry", 409,
                    )
                await conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(spec.namespace)))
                await conn.execute(sql.SQL(
                    "SET LOCAL search_path = {}, pg_catalog"
                ).format(sql.Identifier(spec.namespace)))
                rendered = source.replace(
                    _MODULE_SCHEMA_TOKEN, sql.Identifier(spec.namespace).as_string(conn)
                )
                if execution_profile == "official":
                    for token, identifier in (
                        ("__scope_schema__", schema),
                        ("__scope_identity_table__", identity_table),
                        ("__scope_id_column__", identity_column),
                    ):
                        rendered = rendered.replace(token, sql.Identifier(identifier).as_string(conn))
                await conn.execute(rendered)
                count = await (await conn.execute(
                    "SELECT (SELECT count(*) FROM pg_class c JOIN pg_namespace n "
                    "ON n.oid=c.relnamespace WHERE n.nspname=%s "
                    "AND c.relkind IN ('r','p','v','m','S','f')) + "
                    "(SELECT count(*) FROM pg_type t JOIN pg_namespace n "
                    "ON n.oid=t.typnamespace WHERE n.nspname=%s "
                    "AND t.typtype IN ('d','e')) AS count",
                    (spec.namespace, spec.namespace),
                )).fetchone()
                if count["count"] == 0:
                    raise ForestError(
                        "SCHEMA_BUNDLE_EMPTY", "Storage baseline created no module objects", 422,
                    )
                await conn.execute("SET LOCAL search_path = pg_catalog")
                row = await (await conn.execute(
                    f"INSERT INTO {schema}.schema_bundles("
                    f"{identity_column},schema_id,component_id,schema_version,scope,namespace,"
                    "baseline_sha256,package_digest,repository_contract,execution_profile,state) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'READY') RETURNING *",
                    (
                        scope_id, spec.schema_id, spec.component_id, spec.schema_version,
                        scope, spec.namespace, spec.baseline_sha256, package_digest,
                        spec.repository_contract, execution_profile,
                    ),
                )).fetchone()
                return dict(row)
        except ForestError:
            raise
        except psycopg.Error as exc:
            raise ForestError(
                "SCHEMA_BUNDLE_INSTALL_FAILED",
                f"{scope} module storage baseline could not be installed atomically",
                422,
                details={"schema_id": spec.schema_id},
            ) from exc

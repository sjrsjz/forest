"""Clean Bot authority bootstrap and transactional module schema installation."""

from __future__ import annotations

import hashlib
import re
from importlib.resources import files

import psycopg
from psycopg import sql

from forest.errors import ForestError
from forest.schema_bundles import StorageBundleSpec, validate_storage_baseline

_PACKAGE_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_KERNEL_SCHEMA = "forest_kernel"
_KERNEL_RELATIONS = frozenset({"bot_identity", "schema_bundles"})
_LOCK_DOMAIN = b"forest.schema-bundle/1\0"


def _lock_key(bot_id: str, schema_id: str) -> int:
    digest = hashlib.sha256(_LOCK_DOMAIN + bot_id.encode() + b"\0" + schema_id.encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


def _kernel_baseline() -> tuple[str, str]:
    source = files("forest.storage").joinpath("kernel_baseline.sql").read_bytes()
    try:
        text = source.decode("utf-8")
    except UnicodeError as exc:  # pragma: no cover - packaged source invariant
        raise ForestError("KERNEL_BASELINE_INVALID", "Kernel baseline is not UTF-8") from exc
    return text, hashlib.sha256(source).hexdigest()


class SchemaBundleInstaller:
    """Host-only installer; module workers never receive this repository or DSN."""

    def __init__(self, store) -> None:
        self.store = store

    async def initialize_kernel(self) -> dict:
        """Install the only pre-release kernel baseline into an empty Bot database."""

        source, baseline_sha256 = _kernel_baseline()
        async with self.store.pool.connection() as conn:
            try:
                async with conn.transaction():
                    await conn.execute("SET LOCAL lock_timeout='5s'")
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock(%s)",
                        (_lock_key(self.store.bot_id, "forest.kernel"),),
                    )
                    identity = await self._identity(conn, required=False)
                    if identity is not None:
                        if identity["kernel_baseline_sha256"] != baseline_sha256:
                            raise ForestError(
                                "KERNEL_BASELINE_MISMATCH",
                                "Bot database belongs to another kernel baseline",
                                409,
                            )
                        await self._verify_inventory(conn)
                        return identity
                    await self._require_empty_database(conn)
                    await conn.execute(source)
                    await conn.execute(
                        "INSERT INTO forest_kernel.bot_identity("
                        "bot_id,kernel_baseline_sha256) VALUES (%s,%s)",
                        (self.store.bot_id, baseline_sha256),
                    )
                    return await self._identity(conn)
            except ForestError:
                raise
            except psycopg.Error as exc:
                raise ForestError(
                    "KERNEL_BASELINE_INSTALL_FAILED",
                    "Kernel baseline could not be installed atomically",
                    503,
                ) from exc

    async def install(
        self,
        spec: StorageBundleSpec,
        baseline: bytes,
        package_digest: str,
    ) -> dict:
        """Install an exact bundle or prove that the exact bundle is already READY."""

        source = validate_storage_baseline(baseline, spec.baseline_sha256)
        if not isinstance(package_digest, str) or not _PACKAGE_DIGEST.fullmatch(package_digest):
            raise ForestError("INVALID_STORAGE_BUNDLE", "Storage package digest is invalid", 422)
        async with self.store.pool.connection() as conn:
            try:
                async with conn.transaction():
                    await conn.execute("SET LOCAL lock_timeout='5s'")
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock(%s)",
                        (_lock_key(self.store.bot_id, spec.schema_id),),
                    )
                    await self._identity(conn)
                    existing = await (
                        await conn.execute(
                            "SELECT * FROM forest_kernel.schema_bundles "
                            "WHERE bot_id=%s AND schema_id=%s FOR UPDATE",
                            (self.store.bot_id, spec.schema_id),
                        )
                    ).fetchone()
                    expected = {
                        "component_id": spec.component_id,
                        "schema_version": spec.schema_version,
                        "scope": "bot",
                        "namespace": spec.namespace,
                        "baseline_sha256": spec.baseline_sha256,
                        "package_digest": package_digest,
                        "repository_contract": spec.repository_contract,
                        "state": "READY",
                    }
                    if existing is not None:
                        if all(str(existing[key]) == value for key, value in expected.items()):
                            return dict(existing)
                        raise ForestError(
                            "SCHEMA_BUNDLE_MISMATCH",
                            "Installed schema identity does not match the requested bundle",
                            409,
                        )
                    occupied = await (
                        await conn.execute(
                            "SELECT 1 FROM pg_namespace WHERE nspname=%s", (spec.namespace,)
                        )
                    ).fetchone()
                    if occupied is not None:
                        raise ForestError(
                            "SCHEMA_NAMESPACE_OCCUPIED",
                            "Module schema namespace already exists without a matching ledger entry",
                            409,
                        )
                    await conn.execute(
                        sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(spec.namespace))
                    )
                    await conn.execute(
                        sql.SQL("SET LOCAL search_path = {}, pg_catalog").format(
                            sql.Identifier(spec.namespace)
                        )
                    )
                    await conn.execute(source)
                    count = await self._owned_object_count(conn, spec.namespace)
                    if count == 0:
                        raise ForestError(
                            "SCHEMA_BUNDLE_EMPTY",
                            "Storage baseline did not create any module-owned objects",
                            422,
                        )
                    await conn.execute("SET LOCAL search_path = pg_catalog")
                    row = await (
                        await conn.execute(
                            "INSERT INTO forest_kernel.schema_bundles("
                            "bot_id,schema_id,component_id,schema_version,scope,namespace,"
                            "baseline_sha256,package_digest,repository_contract,state) "
                            "VALUES (%s,%s,%s,%s,'bot',%s,%s,%s,%s,'READY') RETURNING *",
                            (
                                self.store.bot_id,
                                spec.schema_id,
                                spec.component_id,
                                spec.schema_version,
                                spec.namespace,
                                spec.baseline_sha256,
                                package_digest,
                                spec.repository_contract,
                            ),
                        )
                    ).fetchone()
                    return dict(row)
            except ForestError:
                raise
            except psycopg.Error as exc:
                raise ForestError(
                    "SCHEMA_BUNDLE_INSTALL_FAILED",
                    "Module storage baseline could not be installed atomically",
                    422,
                    details={"schema_id": spec.schema_id},
                ) from exc

    async def _identity(self, conn, *, required: bool = True):
        exists = await (
            await conn.execute(
                "SELECT to_regclass('forest_kernel.bot_identity') AS relation"
            )
        ).fetchone()
        if exists["relation"] is None:
            if required:
                raise ForestError(
                    "KERNEL_BASELINE_MISSING",
                    "Module storage requires an initialized kernel baseline",
                    503,
                )
            return None
        row = await (
            await conn.execute(
                "SELECT * FROM forest_kernel.bot_identity WHERE singleton FOR SHARE"
            )
        ).fetchone()
        if row is None or row["bot_id"] != self.store.bot_id:
            raise ForestError(
                "BOT_SCOPE_MISMATCH", "Schema authority belongs to another Bot", 403
            )
        return dict(row)

    async def _require_empty_database(self, conn) -> None:
        namespaces = await (
            await conn.execute(
                "SELECT nspname FROM pg_namespace WHERE nspname NOT IN "
                "('pg_catalog','information_schema','public') "
                "AND nspname NOT LIKE 'pg_toast%' AND nspname NOT LIKE 'pg_temp_%' "
                "AND nspname NOT LIKE 'pg_toast_temp_%' ORDER BY nspname"
            )
        ).fetchall()
        objects = await (
            await conn.execute(
                "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S','f') "
                "UNION ALL SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                "WHERE n.nspname='public' LIMIT 1"
            )
        ).fetchall()
        if namespaces or objects:
            raise ForestError(
                "DATABASE_NOT_EMPTY",
                "A clean kernel baseline requires an empty dedicated database",
                409,
            )

    async def _verify_inventory(self, conn) -> None:
        relations = await (
            await conn.execute(
                "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='forest_kernel' AND c.relkind IN ('r','p','v','m','S','f')"
            )
        ).fetchall()
        if {row["relname"] for row in relations} != _KERNEL_RELATIONS:
            raise ForestError(
                "KERNEL_SCHEMA_TAMPERED",
                "Kernel namespace contains missing or unknown relations",
                409,
            )
        registered = await (
            await conn.execute(
                "SELECT namespace::text AS namespace FROM forest_kernel.schema_bundles"
            )
        ).fetchall()
        expected = {_KERNEL_SCHEMA, *(row["namespace"] for row in registered)}
        actual = await (
            await conn.execute(
                "SELECT nspname FROM pg_namespace WHERE nspname NOT IN "
                "('pg_catalog','information_schema','public') "
                "AND nspname NOT LIKE 'pg_toast%' AND nspname NOT LIKE 'pg_temp_%' "
                "AND nspname NOT LIKE 'pg_toast_temp_%'"
            )
        ).fetchall()
        if {row["nspname"] for row in actual} != expected:
            raise ForestError(
                "SCHEMA_INVENTORY_MISMATCH",
                "Bot database contains an unregistered module namespace",
                409,
            )

    @staticmethod
    async def _owned_object_count(conn, namespace: str) -> int:
        row = await (
            await conn.execute(
                "SELECT ("
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=%s AND c.relkind IN ('r','p','v','m','S','f')"
                ") + (SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace "
                "WHERE n.nspname=%s AND t.typtype IN ('d','e')) AS count",
                (namespace, namespace),
            )
        ).fetchone()
        return row["count"]

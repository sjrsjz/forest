"""Administrator-accepted immutable packages; cached bytes alone never enable code."""

from __future__ import annotations

import asyncio
import hashlib
from uuid import uuid4

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import PackageCatalog, PackageSnapshot


class PackageRegistry:
    def __init__(self, catalog, artifacts: PackageCatalog):
        self.catalog, self.artifacts = catalog, artifacts
        self._slots = asyncio.Semaphore(2)

    @staticmethod
    def _namespace(manifest: dict) -> None:
        identities = [manifest["package_id"], *(c["id"] for c in manifest["components"])]
        if any(value.startswith(("forest.", "adapter.", "core.")) for value in identities):
            raise ForestError("PACKAGE_RESERVED", "This namespace is reserved for built-in components", 422)

    async def registered(self) -> list[str]:
        async with self.catalog.store.pool.connection() as conn:
            rows = await (await conn.execute(
                "SELECT package_digest FROM web_packages ORDER BY created_at,package_digest"
            )).fetchall()
            return [row["package_digest"] for row in rows]

    async def import_archive(self, data: bytes, *, subject_id: str,
                             session_token_hash: str, idempotency_key: str) -> PackageSnapshot:
        if not 1 <= len(idempotency_key) <= 128:
            raise ForestError("IDEMPOTENCY_KEY_REQUIRED", "A stable import key is required", 422)
        store = self.catalog.store
        # Do not hold database locks while validating/decompressing package files.
        async with store.pool.connection() as conn, conn.transaction():
            await self.catalog._authorize(conn, subject_id, session_token_hash)
        if self._slots.locked():
            raise ForestError("PACKAGE_IMPORT_BUSY", "Package import capacity is occupied", 429)
        async with self._slots:
            task = asyncio.create_task(asyncio.to_thread(
                self.artifacts.import_archive, data, check_manifest=self._namespace, bind_version=False,
            ))
            cancelled = False
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    cancelled = True
            if cancelled:
                # Retain admission ownership until the non-cancellable I/O has joined.
                task.exception()
                raise asyncio.CancelledError
            snapshot = task.result()
        package_id, component_id = snapshot.manifest["package_id"], snapshot.component["id"]
        digest = hashlib.sha256(data).hexdigest()
        async with store.pool.connection() as conn, conn.transaction():
            # Global capacity/name serialization precedes account/session locks.
            await conn.execute("SELECT pg_advisory_xact_lock(679330170787)")
            await self.catalog._authorize(conn, subject_id, session_token_hash)
            previous = await (await conn.execute(
                "SELECT input_digest,package_digest FROM web_package_requests "
                "WHERE subject_id=%s AND idempotency_key=%s", (subject_id, idempotency_key),
            )).fetchone()
            if previous is not None:
                if previous["input_digest"] != digest or previous["package_digest"] != snapshot.digest:
                    raise ForestError("IDEMPOTENCY_CONFLICT", "Import key was used for different bytes")
                return snapshot
            existing = await (await conn.execute(
                "SELECT package_digest FROM web_packages WHERE package_id=%s AND version=%s",
                (package_id, snapshot.manifest["version"]),
            )).fetchone()
            if existing is not None and existing["package_digest"] != snapshot.digest:
                raise ForestError("PACKAGE_VERSION_CONFLICT", "This package version has different content")
            if existing is None:
                count = await (await conn.execute("SELECT count(*) AS n FROM web_packages")).fetchone()
                if count["n"] >= 256:
                    raise ForestError("PACKAGE_CAPACITY", "Package registry version capacity reached", 429)
                await conn.execute(
                    "INSERT INTO web_packages(package_digest,package_id,version,component_id,registered_by) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (snapshot.digest, package_id, snapshot.manifest["version"], component_id, subject_id),
                )
            await conn.execute(
                "INSERT INTO web_package_requests(subject_id,idempotency_key,input_digest,package_digest) "
                "VALUES (%s,%s,%s,%s)", (subject_id, idempotency_key, digest, snapshot.digest),
            )
            await conn.execute(
                "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
                "VALUES (%s,%s,'package.import',%s,%s,%s)",
                (str(uuid4()), subject_id, snapshot.digest, str(uuid4()),
                 Jsonb({"package_id": package_id, "version": snapshot.manifest["version"]})),
            )
            await store._active_session(conn, session_token_hash, subject_id)
        return snapshot

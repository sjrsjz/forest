"""Bounded, resumable admission of encrypted Forest backup artifacts."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, AsyncIterator
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from forest.backup_artifact import (
    FORMAT,
    MAX_ARCHIVE,
    MAX_HEADER,
    decrypt_file_with_key,
    inspect_zip,
    recover_data_key,
)
from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import private_directory, private_file

MAX_UPLOAD = MAX_ARCHIVE + MAX_HEADER + 64
_PUBLIC_KEYS = (
    "import_id", "state", "expected_size", "ciphertext_digest", "byte_size", "backup_id",
    "manifest_digest", "database_total", "owner_epoch", "error_code", "expires_at",
    "imported_at", "created_at", "updated_at",
)


def _public(row: dict[str, Any]) -> dict[str, Any]:
    result = {
        key: row[key].isoformat() if isinstance(row.get(key), datetime) else row.get(key)
        for key in _PUBLIC_KEYS
    }
    result["upload_available"] = row.get("state") in {"RESERVED", "UPLOADING"}
    result["finalize_available"] = row.get("state") == "UPLOADED"
    return result


class BackupImportManager:
    """Publish an external ciphertext only after independent full validation."""

    def __init__(
        self, catalog, backups, validator, data_dir: Path, *, lease_seconds: int = 300
    ) -> None:
        if not 60 <= lease_seconds <= 86400:
            raise ValueError("Backup import lease must be between one minute and one day")
        self.catalog = catalog
        self.backups = backups
        self.validator = validator
        self.store = catalog.store
        self.vault = catalog.vault
        self.import_dir = private_directory(Path(data_dir) / "backup-imports")
        self.lease_seconds = lease_seconds
        self.owner_id = "backup-import-worker:" + str(uuid4())
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def _authorize(
        self, conn, subject_id: str, session_token_hash: str,
        expected_password_hash: str | None = None,
    ) -> None:
        account = await self.store._active_account(conn, subject_id, require_admin=True)
        await self.store._active_session(conn, session_token_hash, subject_id)
        if expected_password_hash is not None:
            row = await (await conn.execute(
                "SELECT password_hash FROM web_accounts WHERE id=%s FOR UPDATE", (subject_id,)
            )).fetchone()
            if row is None or row["password_hash"] != expected_password_hash or account["disabled"]:
                raise ForestError(
                    "REAUTHENTICATION_STALE", "Account credentials changed; authenticate again", 409
                )

    async def reserve(
        self, *, subject_id: str, session_token_hash: str, expected_password_hash: str,
        expected_size: int, idempotency_key: str,
    ) -> dict[str, Any]:
        if (
            type(expected_size) is not int or not 1 <= expected_size <= MAX_UPLOAD
            or not 1 <= len(idempotency_key) <= 128
        ):
            raise ForestError("BACKUP_IMPORT_REQUEST_INVALID", "Backup import request is invalid", 422)
        semantic = digest_json({"format": FORMAT, "expected_size": expected_size})
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash, expected_password_hash)
            previous = await (await conn.execute(
                "SELECT * FROM backup_import_jobs WHERE subject_id=%s AND idempotency_key=%s",
                (subject_id, idempotency_key),
            )).fetchone()
            if previous is not None:
                if previous["input_digest"] != semantic:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Import key was used for different input", 409
                    )
                return _public(previous)
            import_id = str(uuid4())
            row = await (await conn.execute(
                "INSERT INTO backup_import_jobs(import_id,subject_id,idempotency_key,input_digest,"
                "expected_size) VALUES (%s,%s,%s,%s,%s) RETURNING *",
                (import_id, subject_id, idempotency_key, semantic, expected_size),
            )).fetchone()
            await conn.execute(
                "INSERT INTO backup_import_events(import_id,owner_epoch,step) "
                "VALUES (%s,0,'RESERVED')", (import_id,),
            )
            await conn.execute(
                "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
                "VALUES (%s,%s,'backup.import.reserve',%s,%s,%s)",
                (str(uuid4()), subject_id, "backup-import:" + import_id, import_id,
                 Jsonb({"import_id": import_id, "expected_size": expected_size})),
            )
            return _public(row)

    async def list(
        self, *, subject_id: str, session_token_hash: str, limit: int = 100,
    ) -> list[dict[str, Any]]:
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ForestError("BACKUP_IMPORT_QUERY_INVALID", "Import limit is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            rows = await (await conn.execute(
                "SELECT * FROM backup_import_jobs ORDER BY created_at DESC,import_id DESC LIMIT %s",
                (limit,),
            )).fetchall()
            await self._authorize(conn, subject_id, session_token_hash)
            return [_public(row) for row in rows]

    async def read(
        self, *, import_id: str, subject_id: str, session_token_hash: str,
    ) -> dict[str, Any]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            row = await (await conn.execute(
                "SELECT * FROM backup_import_jobs WHERE import_id=%s", (import_id,)
            )).fetchone()
            if row is None:
                raise ForestError("BACKUP_IMPORT_NOT_FOUND", "Backup import does not exist", 404)
            await self._authorize(conn, subject_id, session_token_hash)
            return _public(row)

    async def _claim_upload(
        self, *, import_id: str, subject_id: str, session_token_hash: str,
    ) -> dict[str, Any]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            row = await (await conn.execute(
                "SELECT * FROM backup_import_jobs WHERE import_id=%s FOR UPDATE", (import_id,)
            )).fetchone()
            if row is None:
                raise ForestError("BACKUP_IMPORT_NOT_FOUND", "Backup import does not exist", 404)
            now = await self._now(conn)
            if row["expires_at"] <= now:
                raise ForestError("BACKUP_IMPORT_EXPIRED", "Backup import reservation expired", 409)
            if row["state"] == "UPLOADING" and row["lease_expires_at"] > now:
                raise ForestError("BACKUP_IMPORT_BUSY", "Backup upload is already active", 409)
            if row["state"] not in {"RESERVED", "UPLOADING"}:
                raise ForestError("BACKUP_UPLOAD_COMPLETE", "Backup upload is already complete", 409)
            epoch = row["owner_epoch"] + 1
            name = f".job-{import_id}-{epoch}.forest-backup-upload"
            claimed = await (await conn.execute(
                "UPDATE backup_import_jobs SET state='UPLOADING',upload_path=%s,"
                "ciphertext_digest=NULL,byte_size=NULL,owner_id=%s,owner_epoch=%s,"
                "lease_expires_at=clock_timestamp()+(%s*interval '1 second'),"
                "updated_at=clock_timestamp() WHERE import_id=%s RETURNING *",
                (name, self.owner_id, epoch, self.lease_seconds, import_id),
            )).fetchone()
            await conn.execute(
                "INSERT INTO backup_import_events(import_id,owner_epoch,step) "
                "VALUES (%s,%s,'UPLOADING')", (import_id, epoch),
            )
            return claimed

    async def upload(
        self, *, import_id: str, subject_id: str, session_token_hash: str,
        chunks: AsyncIterator[bytes],
    ) -> dict[str, Any]:
        job = await self._claim_upload(
            import_id=import_id, subject_id=subject_id, session_token_hash=session_token_hash
        )
        transfer = asyncio.create_task(
            self._stream_owned(job, subject_id, session_token_hash, chunks),
            name="backup-import-upload-owned",
        )
        renewal = asyncio.create_task(self._renew_lease(job), name="backup-import-upload-lease")
        try:
            done, _ = await asyncio.wait({transfer, renewal}, return_when=asyncio.FIRST_COMPLETED)
            if transfer in done:
                return await transfer
            error = renewal.exception()
            transfer.cancel()
            await asyncio.gather(transfer, return_exceptions=True)
            if error is not None:
                raise error
            raise ForestError("BACKUP_IMPORT_LEASE_LOST", "Backup upload ownership was lost", 409)
        except BaseException:
            await self._abandon_upload(job)
            raise
        finally:
            transfer.cancel()
            renewal.cancel()
            await asyncio.gather(transfer, renewal, return_exceptions=True)

    async def _stream_owned(
        self, job: dict[str, Any], subject_id: str, session_token_hash: str,
        chunks: AsyncIterator[bytes],
    ) -> dict[str, Any]:
        path = self._upload_path(job["upload_path"])
        await asyncio.to_thread(self._discard_older_uploads, job)
        if path.exists():
            path.unlink()
        private_file(path)
        digest, size = hashlib.sha256(), 0
        try:
            with path.open("wb") as stream:
                async for chunk in chunks:
                    if not chunk:
                        continue
                    size += len(chunk)
                    if size > job["expected_size"]:
                        raise ForestError(
                            "BACKUP_UPLOAD_SIZE_MISMATCH", "Uploaded backup size is inconsistent", 413
                        )
                    digest.update(chunk)
                    await asyncio.to_thread(stream.write, chunk)
                await asyncio.to_thread(stream.flush)
                await asyncio.to_thread(os.fsync, stream.fileno())
            if size != job["expected_size"]:
                raise ForestError(
                    "BACKUP_UPLOAD_SIZE_MISMATCH", "Uploaded backup size is inconsistent", 422
                )
            ciphertext_digest = "sha256:" + digest.hexdigest()
            async with self.store.pool.connection() as conn, conn.transaction():
                await self._authorize(conn, subject_id, session_token_hash)
                updated = await (await conn.execute(
                    "UPDATE backup_import_jobs SET state='UPLOADED',ciphertext_digest=%s,"
                    "byte_size=%s,owner_id=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() "
                    "WHERE import_id=%s AND state='UPLOADING' AND owner_id=%s AND owner_epoch=%s "
                    "AND lease_expires_at>clock_timestamp() RETURNING *",
                    (ciphertext_digest, size, job["import_id"], self.owner_id,
                     job["owner_epoch"]),
                )).fetchone()
                if updated is None:
                    raise ForestError(
                        "BACKUP_IMPORT_LEASE_LOST", "Backup upload ownership was lost", 409
                    )
                await conn.execute(
                    "INSERT INTO backup_import_events(import_id,owner_epoch,step,details) "
                    "VALUES (%s,%s,'UPLOADED',%s)",
                    (job["import_id"], job["owner_epoch"], Jsonb({
                        "ciphertext_digest": ciphertext_digest, "byte_size": size,
                    })),
                )
                return _public(updated)
        except BaseException:
            path.unlink(missing_ok=True)
            raise

    async def _abandon_upload(self, job: dict[str, Any]) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            reset = await (await conn.execute(
                "UPDATE backup_import_jobs SET state='RESERVED',upload_path=NULL,"
                "ciphertext_digest=NULL,byte_size=NULL,owner_id=NULL,lease_expires_at=NULL,"
                "updated_at=clock_timestamp() WHERE import_id=%s AND state='UPLOADING' "
                "AND owner_id=%s AND owner_epoch=%s RETURNING 1",
                (job["import_id"], self.owner_id, job["owner_epoch"]),
            )).fetchone()
            if reset is not None:
                await conn.execute(
                    "INSERT INTO backup_import_events(import_id,owner_epoch,step) "
                    "VALUES (%s,%s,'UPLOAD_ABORTED')", (job["import_id"], job["owner_epoch"]),
                )
        self._upload_path(job["upload_path"]).unlink(missing_ok=True)

    async def queue(
        self, *, import_id: str, subject_id: str, session_token_hash: str,
        expected_password_hash: str, passphrase: str, passphrase_digest: str,
    ) -> dict[str, Any]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash, expected_password_hash)
            row = await (await conn.execute(
                "SELECT * FROM backup_import_jobs WHERE import_id=%s", (import_id,)
            )).fetchone()
        if row is None:
            raise ForestError("BACKUP_IMPORT_NOT_FOUND", "Backup import does not exist", 404)
        if row["state"] in {"QUEUED", "IMPORTING", "IMPORTED"}:
            if row["passphrase_digest"] != passphrase_digest:
                raise ForestError("BACKUP_IMPORT_FINALIZE_CONFLICT", "Import was finalized differently", 409)
            return _public(row)
        if row["state"] != "UPLOADED":
            raise ForestError("BACKUP_IMPORT_NOT_UPLOADED", "Backup upload is not complete", 409)
        path = self._upload_path(row["upload_path"])
        if not path.is_file() or path.is_symlink():
            raise ForestError("BACKUP_STORAGE_ERROR", "Uploaded backup is unavailable", 503)
        actual = await asyncio.to_thread(self.backups._file_digest, path)
        if actual != row["ciphertext_digest"] or path.stat().st_size != row["byte_size"]:
            raise ForestError("BACKUP_INTEGRITY_FAILED", "Uploaded backup integrity failed", 409)
        header, archive_key = await asyncio.to_thread(recover_data_key, path, passphrase)
        try:
            parsed_id = str(UUID(header["backup_id"]))
        except (ValueError, TypeError, KeyError):
            raise ForestError("BACKUP_ID_INVALID", "Imported backup identity is invalid", 422) from None
        if parsed_id != header["backup_id"]:
            raise ForestError("BACKUP_ID_INVALID", "Imported backup identity is invalid", 422)
        encrypted_key = self.vault.encrypt(base64.urlsafe_b64encode(archive_key).decode("ascii"))
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash, expected_password_hash)
            current = await (await conn.execute(
                "SELECT * FROM backup_import_jobs WHERE import_id=%s FOR UPDATE", (import_id,)
            )).fetchone()
            if current["state"] != "UPLOADED" or (
                current["ciphertext_digest"] != actual or current["byte_size"] != path.stat().st_size
            ):
                raise ForestError("BACKUP_IMPORT_CHANGED", "Backup import changed during finalize", 409)
            queued = await (await conn.execute(
                "UPDATE backup_import_jobs SET state='QUEUED',passphrase_digest=%s,"
                "archive_key_ciphertext=%s,backup_id=%s,updated_at=clock_timestamp() "
                "WHERE import_id=%s RETURNING *",
                (passphrase_digest, encrypted_key, parsed_id, import_id),
            )).fetchone()
            await conn.execute(
                "INSERT INTO backup_import_events(import_id,owner_epoch,step) "
                "VALUES (%s,%s,'QUEUED')", (import_id, queued["owner_epoch"]),
            )
            await conn.execute(
                "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
                "VALUES (%s,%s,'backup.import.finalize',%s,%s,%s)",
                (str(uuid4()), subject_id, "backup-import:" + import_id, import_id,
                 Jsonb({"import_id": import_id, "backup_id": parsed_id,
                        "ciphertext_digest": actual})),
            )
            return _public(queued)

    async def _claim(self) -> dict[str, Any] | None:
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(
                "SELECT * FROM backup_import_jobs WHERE state='QUEUED' OR "
                "(state='IMPORTING' AND lease_expires_at<=clock_timestamp()) "
                "ORDER BY (state='IMPORTING') DESC,created_at,import_id "
                "FOR UPDATE SKIP LOCKED LIMIT 1"
            )).fetchone()
            if row is None:
                return None
            claimed = await (await conn.execute(
                "UPDATE backup_import_jobs SET state='IMPORTING',owner_id=%s,"
                "owner_epoch=owner_epoch+1,lease_expires_at=clock_timestamp()+"
                "(%s*interval '1 second'),error_code=NULL,updated_at=clock_timestamp() "
                "WHERE import_id=%s RETURNING *",
                (self.owner_id, self.lease_seconds, row["import_id"]),
            )).fetchone()
            await conn.execute(
                "INSERT INTO backup_import_events(import_id,owner_epoch,step) "
                "VALUES (%s,%s,'IMPORTING')", (claimed["import_id"], claimed["owner_epoch"]),
            )
            return claimed

    async def _heartbeat(self, job: dict[str, Any]) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            updated = await (await conn.execute(
                "UPDATE backup_import_jobs SET lease_expires_at=clock_timestamp()+"
                "(%s*interval '1 second'),updated_at=clock_timestamp() "
                "WHERE import_id=%s AND owner_id=%s AND owner_epoch=%s "
                "AND state IN ('UPLOADING','IMPORTING') AND lease_expires_at>clock_timestamp() "
                "RETURNING 1",
                (self.lease_seconds, job["import_id"], self.owner_id, job["owner_epoch"]),
            )).fetchone()
            if updated is None:
                raise ForestError("BACKUP_IMPORT_LEASE_LOST", "Backup import ownership was lost", 409)

    async def _renew_lease(self, job: dict[str, Any]) -> None:
        interval = max(10.0, self.lease_seconds / 3)
        while True:
            await asyncio.sleep(interval)
            await self._heartbeat(job)

    async def run_once(self) -> dict[str, Any] | None:
        reaped = await self._reap_one()
        if reaped is not None:
            return _public(reaped)
        expired = await self._expire_one()
        if expired is not None:
            return expired
        job = await self._claim()
        if job is None:
            return None
        work = asyncio.create_task(self._import_owned(job), name="backup-import-owned")
        renewal = asyncio.create_task(self._renew_lease(job), name="backup-import-lease")
        try:
            done, _ = await asyncio.wait({work, renewal}, return_when=asyncio.FIRST_COMPLETED)
            if work in done:
                return await work
            error = renewal.exception()
            work.cancel()
            await asyncio.gather(work, return_exceptions=True)
            if error is not None:
                raise error
            raise ForestError("BACKUP_IMPORT_LEASE_LOST", "Backup import ownership was lost", 409)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            code = exc.code if isinstance(exc, ForestError) else "BACKUP_IMPORT_FAILED"
            async with self.store.pool.connection() as conn, conn.transaction():
                failed = await (await conn.execute(
                    "UPDATE backup_import_jobs SET state='FAILED',owner_id=NULL,"
                    "lease_expires_at=NULL,error_code=%s,updated_at=clock_timestamp() "
                    "WHERE import_id=%s AND state='IMPORTING' AND owner_id=%s "
                    "AND owner_epoch=%s RETURNING *",
                    (code, job["import_id"], self.owner_id, job["owner_epoch"]),
                )).fetchone()
                if failed is not None:
                    await conn.execute(
                        "INSERT INTO backup_import_events(import_id,owner_epoch,step,details) "
                        "VALUES (%s,%s,'FAILED',%s)",
                        (job["import_id"], job["owner_epoch"], Jsonb({"error_code": code})),
                    )
            if failed is not None:
                await self._try_cleanup_terminal_artifact(failed)
            return _public(failed) if failed is not None else await self.read_internal(job["import_id"])
        finally:
            work.cancel()
            renewal.cancel()
            await asyncio.gather(work, renewal, return_exceptions=True)

    async def run_forever(self) -> None:
        self.closed = False
        while not self.closed:
            result = await self.run_once()
            if result is None:
                await asyncio.sleep(1)

    async def read_internal(self, import_id: str) -> dict[str, Any]:
        async with self.store.pool.connection() as conn:
            return await (await conn.execute(
                "SELECT * FROM backup_import_jobs WHERE import_id=%s", (import_id,)
            )).fetchone()

    async def _import_owned(self, job: dict[str, Any]) -> dict[str, Any]:
        source = await self._source(job)
        job_root = self.import_dir / f".verify-{job['import_id']}-{job['owner_epoch']}"
        await asyncio.to_thread(self.backups._reset_job_root, job_root)
        plain = job_root / "payload.zip"
        try:
            key = base64.urlsafe_b64decode(self.vault.decrypt(job["archive_key_ciphertext"]))
            header = await asyncio.to_thread(decrypt_file_with_key, source, plain, key)
            if header["backup_id"] != job["backup_id"]:
                raise ForestError("BACKUP_HEADER_CHANGED", "Backup header changed during import", 409)
            manifest = await asyncio.to_thread(inspect_zip, plain)
            manifest_digest = await asyncio.to_thread(self.validator._manifest_digest, plain)
            targets = [{
                "database_scope": database["scope"],
                "bot_id": database.get("bot_id") or "",
                "content_digest": database.get("content_digest"),
                "migration_digest": database.get("migration_digest"),
                "deletion_epoch": database.get("deletion_epoch"),
            } for database in manifest.get("databases", []) if isinstance(database, dict)]
            await self.validator._validate_manifest(
                {"backup_id": job["backup_id"], "artifact_digest": job["ciphertext_digest"]},
                header, manifest, manifest_digest, manifest_digest, job["ciphertext_digest"],
                targets=targets, publish_restore=False,
            )
            return await self._publish(job, source, header, manifest, manifest_digest)
        finally:
            await asyncio.to_thread(self.backups._remove_job_root, job_root)

    async def _publish(
        self, job: dict[str, Any], source: Path, header: dict[str, Any],
        manifest: dict[str, Any], manifest_digest: str,
    ) -> dict[str, Any]:
        backup_id = job["backup_id"]
        final = self.backups.backup_dir / f"{backup_id}.0.forest-backup"
        databases = manifest["databases"]
        roster = manifest["roster"]
        async with self.store.pool.connection() as conn, conn.transaction():
            current = await (await conn.execute(
                "SELECT * FROM backup_import_jobs WHERE import_id=%s FOR UPDATE",
                (job["import_id"],),
            )).fetchone()
            now = await self._now(conn)
            if (
                current is None or current["state"] != "IMPORTING"
                or current["owner_id"] != self.owner_id
                or current["owner_epoch"] != job["owner_epoch"]
                or current["lease_expires_at"] <= now
            ):
                raise ForestError("BACKUP_IMPORT_LEASE_LOST", "Backup import ownership was lost", 409)
            existing = await (await conn.execute(
                "SELECT j.state,a.relative_path,a.ciphertext_digest,a.manifest_digest,a.deleted_at,"
                "a.byte_size,a.expires_at "
                "FROM backup_jobs j LEFT JOIN backup_artifacts a USING(backup_id) "
                "WHERE j.backup_id=%s FOR UPDATE OF j", (backup_id,)
            )).fetchone()
            deduplicated = existing is not None
            if existing is not None:
                if (
                    existing["state"] != "READY" or existing["relative_path"] is None
                    or existing["deleted_at"] is not None or existing["expires_at"] is None
                    or existing["expires_at"] <= now
                    or existing["ciphertext_digest"] != job["ciphertext_digest"]
                    or existing["manifest_digest"] != manifest_digest
                    or existing["byte_size"] != job["byte_size"]
                ):
                    raise ForestError(
                        "BACKUP_ID_CONFLICT", "Backup identity already names different content", 409
                    )
                registered = (self.backups.backup_dir / existing["relative_path"]).resolve()
                if (
                    registered.parent != self.backups.backup_dir.resolve()
                    or not registered.is_file() or registered.is_symlink()
                    or await asyncio.to_thread(self.backups._file_digest, registered)
                    != existing["ciphertext_digest"]
                ):
                    raise ForestError(
                        "BACKUP_INTEGRITY_FAILED", "Existing backup artifact is unavailable", 409
                    )
            else:
                if source != final:
                    if final.exists():
                        if await asyncio.to_thread(self.backups._file_digest, final) != job["ciphertext_digest"]:
                            raise ForestError(
                                "BACKUP_ID_CONFLICT", "Backup destination contains different content", 409
                            )
                        source.unlink(missing_ok=True)
                    else:
                        await asyncio.to_thread(os.replace, source, final)
                        await asyncio.to_thread(self.backups._sync_directory, self.backups.backup_dir)
                include_packages = manifest["includes"]["packages"]
                await conn.execute(
                    "INSERT INTO backup_jobs(backup_id,subject_id,idempotency_key,input_digest,"
                    "include_packages,state,archive_key_ciphertext,archive_header,database_total,"
                    "database_completed,roster_start_digest,roster_end_digest) "
                    "VALUES (%s,%s,%s,%s,%s,'READY',%s,%s,%s,%s,%s,%s)",
                    (backup_id, job["subject_id"], "import:" + job["import_id"],
                     digest_json({"import_id": job["import_id"],
                                  "ciphertext_digest": job["ciphertext_digest"]}),
                     include_packages, job["archive_key_ciphertext"], Jsonb(header), len(databases),
                     len(databases), roster["start_digest"], roster["end_digest"]),
                )
                for database in databases:
                    await conn.execute(
                        "INSERT INTO backup_database_cuts(backup_id,database_scope,bot_id,"
                        "identity_digest,migration_digest,transaction_snapshot,wal_lsn,deletion_epoch,"
                        "member_count,byte_size,content_digest,captured_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        self.backups._cut_row(backup_id, database),
                    )
                expires = datetime.now(UTC) + timedelta(days=30)
                await conn.execute(
                    "INSERT INTO backup_artifacts(backup_id,relative_path,ciphertext_digest,byte_size,"
                    "manifest_digest,expires_at) VALUES (%s,%s,%s,%s,%s,%s)",
                    (backup_id, final.name, job["ciphertext_digest"], job["byte_size"],
                     manifest_digest, expires),
                )
                await conn.execute(
                    "INSERT INTO backup_events(backup_id,owner_epoch,step,details) "
                    "VALUES (%s,0,'IMPORTED',%s)",
                    (backup_id, Jsonb({"import_id": job["import_id"]})),
                )
            imported = await (await conn.execute(
                "UPDATE backup_import_jobs SET state='IMPORTED',owner_id=NULL,lease_expires_at=NULL,"
                "manifest_digest=%s,database_total=%s,imported_at=clock_timestamp(),error_code=NULL,"
                "updated_at=clock_timestamp() WHERE import_id=%s RETURNING *",
                (manifest_digest, len(databases), job["import_id"]),
            )).fetchone()
            await conn.execute(
                "INSERT INTO backup_import_events(import_id,owner_epoch,step,details) "
                "VALUES (%s,%s,'IMPORTED',%s)",
                (job["import_id"], job["owner_epoch"], Jsonb({
                    "backup_id": backup_id, "manifest_digest": manifest_digest,
                    "deduplicated": deduplicated,
                })),
            )
            await conn.execute(
                "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
                "VALUES (%s,%s,'backup.import.completed',%s,%s,%s)",
                (str(uuid4()), job["subject_id"], "backup:" + backup_id, job["import_id"],
                 Jsonb({"import_id": job["import_id"], "backup_id": backup_id,
                        "ciphertext_digest": job["ciphertext_digest"],
                        "manifest_digest": manifest_digest, "database_total": len(databases),
                        "deduplicated": deduplicated})),
            )
        await self._try_cleanup_terminal_artifact(imported)
        return _public(imported)

    async def _source(self, job: dict[str, Any]) -> Path:
        upload = self._upload_path(job["upload_path"])
        final = self.backups.backup_dir / f"{job['backup_id']}.0.forest-backup"
        source = upload if upload.is_file() and not upload.is_symlink() else final
        if not source.is_file() or source.is_symlink():
            raise ForestError("BACKUP_STORAGE_ERROR", "Imported backup artifact is unavailable", 503)
        actual = await asyncio.to_thread(self.backups._file_digest, source)
        if actual != job["ciphertext_digest"] or source.stat().st_size != job["byte_size"]:
            raise ForestError("BACKUP_INTEGRITY_FAILED", "Imported backup integrity failed", 409)
        return source

    async def _expire_one(self) -> dict[str, Any] | None:
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(
                "SELECT * FROM backup_import_jobs WHERE state IN ('RESERVED','UPLOADED') "
                "AND expires_at<=clock_timestamp() ORDER BY expires_at,import_id "
                "FOR UPDATE SKIP LOCKED LIMIT 1"
            )).fetchone()
            if row is None:
                return None
            expired = await (await conn.execute(
                "UPDATE backup_import_jobs SET state='EXPIRED',updated_at=clock_timestamp() "
                "WHERE import_id=%s RETURNING *", (row["import_id"],)
            )).fetchone()
            await conn.execute(
                "INSERT INTO backup_import_events(import_id,owner_epoch,step) "
                "VALUES (%s,%s,'EXPIRED')", (row["import_id"], row["owner_epoch"]),
            )
            await conn.execute(
                "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
                "VALUES (%s,%s,'backup.import.expired',%s,%s,%s)",
                (str(uuid4()), row["subject_id"], "backup-import:" + row["import_id"],
                 row["import_id"], Jsonb({"import_id": row["import_id"]})),
            )
        if row["upload_path"]:
            await self._try_cleanup_terminal_artifact(expired)
        return _public(expired)

    async def _reap_one(self) -> dict[str, Any] | None:
        async with self.store.pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT j.* FROM backup_import_jobs j "
                "LEFT JOIN backup_import_cleanup_receipts r USING(import_id) "
                "WHERE j.state IN ('IMPORTED','FAILED','EXPIRED') AND j.upload_path IS NOT NULL "
                "AND r.import_id IS NULL ORDER BY j.updated_at,j.import_id LIMIT 1"
            )).fetchone()
        if row is None:
            return None
        cleaned = await self._try_cleanup_terminal_artifact(row)
        return row if cleaned else None

    async def _try_cleanup_terminal_artifact(self, job: dict[str, Any]) -> bool:
        try:
            await self._cleanup_terminal_artifact(job)
        except Exception:
            # The terminal fact is already durable. A later run retries the missing receipt.
            return False
        return True

    async def _cleanup_terminal_artifact(self, job: dict[str, Any]) -> None:
        upload = self._upload_path(job["upload_path"])
        upload_result = await asyncio.to_thread(
            self._delete_exact, upload, job["ciphertext_digest"], job["byte_size"]
        )
        final_result = "NOT_APPLICABLE"
        if job["state"] == "FAILED" and job.get("backup_id"):
            final = self.backups.backup_dir / f"{job['backup_id']}.0.forest-backup"
            async with self.store.pool.connection() as conn:
                owned = await (await conn.execute(
                    "SELECT 1 FROM backup_artifacts WHERE backup_id=%s AND relative_path=%s",
                    (job["backup_id"], final.name),
                )).fetchone()
            final_result = "OWNED" if owned is not None else await asyncio.to_thread(
                self._delete_exact, final, job["ciphertext_digest"], job["byte_size"]
            )
        details = {"upload_result": upload_result, "final_result": final_result}
        async with self.store.pool.connection() as conn, conn.transaction():
            inserted = await (await conn.execute(
                "INSERT INTO backup_import_cleanup_receipts(cleanup_id,import_id,upload_path,"
                "ciphertext_digest,details) VALUES (%s,%s,%s,%s,%s) "
                "ON CONFLICT(import_id) DO NOTHING RETURNING 1",
                (str(uuid4()), job["import_id"], job["upload_path"],
                 job["ciphertext_digest"], Jsonb(details)),
            )).fetchone()
            if inserted is not None:
                await conn.execute(
                    "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,"
                    "details) VALUES (%s,%s,'backup.import.cleanup',%s,%s,%s)",
                    (str(uuid4()), job["subject_id"], "backup-import:" + job["import_id"],
                     job["import_id"], Jsonb({"import_id": job["import_id"], **details})),
                )

    def _delete_exact(self, path: Path, digest: str, size: int) -> str:
        if not path.exists():
            return "ABSENT"
        if path.is_symlink() or not path.is_file():
            return "CONFLICT"
        if path.stat().st_size != size or self.backups._file_digest(path) != digest:
            return "CONFLICT"
        path.unlink()
        return "DELETED"

    def _upload_path(self, name: str) -> Path:
        path = (self.import_dir / name).resolve()
        if path.parent != self.import_dir.resolve():
            raise ForestError("BACKUP_STORAGE_ERROR", "Backup import path is unsafe", 500)
        return path

    def _discard_older_uploads(self, job: dict[str, Any]) -> None:
        prefix = f".job-{job['import_id']}-"
        current = job["upload_path"]
        for candidate in self.import_dir.iterdir():
            if (
                candidate.name != current and candidate.name.startswith(prefix)
                and candidate.name.endswith(".forest-backup-upload")
                and candidate.is_file() and not candidate.is_symlink()
            ):
                middle = candidate.name[len(prefix):-len(".forest-backup-upload")]
                if middle.isdecimal() and int(middle) < job["owner_epoch"]:
                    candidate.unlink(missing_ok=True)

    @staticmethod
    async def _now(conn):
        return (await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone())["now"]

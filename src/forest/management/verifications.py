"""Durable read-only validation receipts for exact encrypted backup artifacts."""

from __future__ import annotations

import asyncio
import base64
import hmac
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from psycopg.types.json import Jsonb

from forest.backup_artifact import decrypt_file_with_key, inspect_zip, recover_data_key
from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import private_directory

_SELECT = "SELECT * FROM backup_verification_jobs "
_PUBLIC = (
    "verification_id", "backup_id", "state", "owner_epoch", "manifest_digest",
    "database_total", "member_total", "plaintext_size", "error_code", "verified_at",
    "expires_at", "created_at", "updated_at",
)


def _public(row: dict[str, Any]) -> dict[str, Any]:
    result = {
        key: value.isoformat() if isinstance((value := row.get(key)), datetime) else value
        for key in _PUBLIC
    }
    result["receipt_valid"] = bool(
        row.get("state") == "VERIFIED" and row.get("expires_at")
        and row["expires_at"] > datetime.now(UTC)
    )
    return result


class BackupVerificationManager:
    """Validate one artifact without creating, registering, or connecting to restore databases."""

    def __init__(
        self, catalog, backups, validator, data_dir: Path, *, lease_seconds: int = 3600,
        receipt_seconds: int = 86400,
    ):
        if not 60 <= lease_seconds <= 86400:
            raise ValueError("Verification lease must be between one minute and one day")
        if not 300 <= receipt_seconds <= 604800:
            raise ValueError("Verification receipt must last between five minutes and seven days")
        self.catalog = catalog
        self.backups = backups
        self.validator = validator
        self.store = catalog.store
        self.vault = catalog.vault
        self.verify_dir = private_directory(Path(data_dir) / "backup-verifications")
        self.lease_seconds = lease_seconds
        self.receipt_seconds = receipt_seconds
        self.owner_id = "backup-verification:" + str(uuid4())
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

    async def create(
        self, *, backup_id: str, subject_id: str, session_token_hash: str,
        expected_password_hash: str, passphrase: str, passphrase_digest: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        if not 1 <= len(idempotency_key) <= 128:
            raise ForestError("VERIFICATION_REQUEST_INVALID", "Verification request is invalid", 422)
        semantic = digest_json({
            "backup_id": backup_id, "passphrase_digest": passphrase_digest,
            "validator": "forest.backup.verify.v1",
        })
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash, expected_password_hash)
            previous = await (await conn.execute(
                _SELECT + "WHERE subject_id=%s AND idempotency_key=%s",
                (subject_id, idempotency_key),
            )).fetchone()
            if previous is not None:
                if previous["input_digest"] != semantic:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Verification key was used for different input", 409
                    )
                return _public(previous)

        path, artifact = await self.backups.artifact(
            backup_id=backup_id, subject_id=subject_id,
            session_token_hash=session_token_hash,
        )
        try:
            header, recovered_key = await asyncio.to_thread(recover_data_key, path, passphrase)
        except ForestError:
            raise
        except OSError:
            raise ForestError(
                "BACKUP_VERIFICATION_UNAVAILABLE", "Backup cannot be verified", 409
            ) from None
        if header["backup_id"] != backup_id:
            raise ForestError("BACKUP_IDENTITY_MISMATCH", "Backup identity is invalid", 409)
        async with self.store.pool.connection() as conn:
            source = await (await conn.execute(
                "SELECT archive_key_ciphertext FROM backup_jobs WHERE backup_id=%s", (backup_id,)
            )).fetchone()
        if source is None:
            raise ForestError("BACKUP_NOT_FOUND", "Backup does not exist", 404)
        saved_key = base64.urlsafe_b64decode(self.vault.decrypt(source["archive_key_ciphertext"]))
        if not hmac.compare_digest(recovered_key, saved_key):
            raise ForestError(
                "BACKUP_PASSPHRASE_INVALID", "Recovery passphrase or archive is invalid", 403
            )
        verification_id = str(uuid4())
        encrypted_key = self.vault.encrypt(base64.urlsafe_b64encode(recovered_key).decode("ascii"))
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(679330170796)")
            await self._authorize(conn, subject_id, session_token_hash, expected_password_hash)
            previous = await (await conn.execute(
                _SELECT + "WHERE subject_id=%s AND idempotency_key=%s",
                (subject_id, idempotency_key),
            )).fetchone()
            if previous is not None:
                if previous["input_digest"] != semantic:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Verification key was used for different input", 409
                    )
                return _public(previous)
            current = await (await conn.execute(
                "SELECT j.state,a.ciphertext_digest,a.deleted_at,a.expires_at "
                "FROM backup_jobs j JOIN backup_artifacts a USING(backup_id) "
                "WHERE j.backup_id=%s FOR UPDATE OF j,a", (backup_id,)
            )).fetchone()
            if (
                current is None or current["state"] != "READY" or current["deleted_at"] is not None
                or current["expires_at"] <= await self._now(conn)
                or current["ciphertext_digest"] != artifact["ciphertext_digest"]
            ):
                raise ForestError(
                    "BACKUP_VERIFICATION_UNAVAILABLE", "Backup cannot be verified", 409
                )
            cuts = await (await conn.execute(
                "SELECT count(*) AS n FROM backup_database_cuts WHERE backup_id=%s", (backup_id,)
            )).fetchone()
            if not 1 <= cuts["n"] <= 1024:
                raise ForestError("BACKUP_MANIFEST_INVALID", "Backup database roster is invalid", 409)
            row = await (await conn.execute(
                "INSERT INTO backup_verification_jobs(verification_id,backup_id,subject_id,"
                "idempotency_key,input_digest,artifact_digest,archive_key_ciphertext,database_total) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                (verification_id, backup_id, subject_id, idempotency_key, semantic,
                 artifact["ciphertext_digest"], encrypted_key, cuts["n"]),
            )).fetchone()
            await conn.execute(
                "INSERT INTO backup_verification_events(verification_id,owner_epoch,step) "
                "VALUES (%s,0,'QUEUED')", (verification_id,)
            )
            await conn.execute(
                "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
                "VALUES (%s,%s,'backup.verify.create',%s,%s,%s)",
                (str(uuid4()), subject_id, "backup:" + backup_id, verification_id,
                 Jsonb({"verification_id": verification_id, "backup_id": backup_id})),
            )
            return _public(row)

    async def list(
        self, *, subject_id: str, session_token_hash: str, limit: int = 100,
    ) -> list[dict[str, Any]]:
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ForestError("VERIFICATION_QUERY_INVALID", "Verification limit is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            rows = await (await conn.execute(
                _SELECT + "ORDER BY created_at DESC,verification_id DESC LIMIT %s", (limit,)
            )).fetchall()
            await self._authorize(conn, subject_id, session_token_hash)
            return [_public(row) for row in rows]

    async def read(
        self, *, verification_id: str, subject_id: str, session_token_hash: str,
    ) -> dict[str, Any]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            row = await (await conn.execute(
                _SELECT + "WHERE verification_id=%s", (verification_id,)
            )).fetchone()
            if row is None:
                raise ForestError("VERIFICATION_NOT_FOUND", "Verification does not exist", 404)
            await self._authorize(conn, subject_id, session_token_hash)
            return _public(row)

    async def _expire(self) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(
                "SELECT * FROM backup_verification_jobs WHERE state='VERIFIED' "
                "AND expires_at<=clock_timestamp() ORDER BY expires_at,verification_id "
                "FOR UPDATE SKIP LOCKED LIMIT 1"
            )).fetchone()
            if row is not None:
                await conn.execute(
                    "UPDATE backup_verification_jobs SET state='EXPIRED',"
                    "updated_at=clock_timestamp() WHERE verification_id=%s",
                    (row["verification_id"],),
                )
                await conn.execute(
                    "INSERT INTO backup_verification_events(verification_id,owner_epoch,step) "
                    "VALUES (%s,%s,'EXPIRED')", (row["verification_id"], row["owner_epoch"]),
                )

    async def _claim(self) -> dict[str, Any] | None:
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(
                "SELECT * FROM backup_verification_jobs WHERE state='QUEUED' OR "
                "(state='VERIFYING' AND lease_expires_at<=clock_timestamp()) "
                "ORDER BY (state='VERIFYING') DESC,created_at,verification_id "
                "FOR UPDATE SKIP LOCKED LIMIT 1"
            )).fetchone()
            if row is None:
                return None
            claimed = await (await conn.execute(
                "UPDATE backup_verification_jobs SET state='VERIFYING',owner_id=%s,"
                "owner_epoch=owner_epoch+1,lease_expires_at=clock_timestamp()+"
                "(%s*interval '1 second'),manifest_digest=NULL,member_total=NULL,"
                "plaintext_size=NULL,error_code=NULL,updated_at=clock_timestamp() "
                "WHERE verification_id=%s RETURNING *",
                (self.owner_id, self.lease_seconds, row["verification_id"]),
            )).fetchone()
            await conn.execute(
                "INSERT INTO backup_verification_events(verification_id,owner_epoch,step) "
                "VALUES (%s,%s,'VERIFYING')",
                (claimed["verification_id"], claimed["owner_epoch"]),
            )
            return claimed

    async def _heartbeat(self, job: dict[str, Any]) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            changed = await (await conn.execute(
                "UPDATE backup_verification_jobs SET lease_expires_at=clock_timestamp()+"
                "(%s*interval '1 second'),updated_at=clock_timestamp() "
                "WHERE verification_id=%s AND state='VERIFYING' AND owner_id=%s "
                "AND owner_epoch=%s AND lease_expires_at>clock_timestamp() RETURNING 1",
                (self.lease_seconds, job["verification_id"], self.owner_id, job["owner_epoch"]),
            )).fetchone()
            if changed is None:
                raise ForestError("VERIFICATION_LEASE_LOST", "Verification ownership was lost", 409)

    async def _renew(self, job: dict[str, Any]) -> None:
        while True:
            await asyncio.sleep(max(10.0, self.lease_seconds / 3))
            await self._heartbeat(job)

    async def run_once(self) -> dict[str, Any] | None:
        await self._expire()
        job = await self._claim()
        if job is None:
            return None
        try:
            return await self._verify(job)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            code = exc.code if isinstance(exc, ForestError) else "BACKUP_VERIFICATION_FAILED"
            async with self.store.pool.connection() as conn, conn.transaction():
                failed = await (await conn.execute(
                    "UPDATE backup_verification_jobs SET state='FAILED',owner_id=NULL,"
                    "lease_expires_at=NULL,error_code=%s,updated_at=clock_timestamp() "
                    "WHERE verification_id=%s AND state='VERIFYING' AND owner_id=%s "
                    "AND owner_epoch=%s RETURNING *",
                    (code, job["verification_id"], self.owner_id, job["owner_epoch"]),
                )).fetchone()
                if failed is not None:
                    await conn.execute(
                        "INSERT INTO backup_verification_events(verification_id,owner_epoch,"
                        "step,details) VALUES (%s,%s,'FAILED',%s)",
                        (job["verification_id"], job["owner_epoch"],
                         Jsonb({"error_code": code})),
                    )
                    return _public(failed)
            return await self._read_internal(job["verification_id"])

    async def run_forever(self) -> None:
        self.closed = False
        while not self.closed:
            result = await self.run_once()
            if result is None:
                await asyncio.sleep(1)

    async def _verify(self, job: dict[str, Any]) -> dict[str, Any]:
        work = asyncio.create_task(self._verify_owned(job), name="backup-verification-owned")
        renewal = asyncio.create_task(self._renew(job), name="backup-verification-lease")
        try:
            done, _ = await asyncio.wait({work, renewal}, return_when=asyncio.FIRST_COMPLETED)
            if work in done:
                return await work
            error = renewal.exception()
            work.cancel()
            await asyncio.gather(work, return_exceptions=True)
            if error is not None:
                raise error
            raise ForestError("VERIFICATION_LEASE_LOST", "Verification ownership was lost", 409)
        finally:
            work.cancel()
            renewal.cancel()
            await asyncio.gather(work, renewal, return_exceptions=True)

    async def _verify_owned(self, job: dict[str, Any]) -> dict[str, Any]:
        job_root = self.verify_dir / (
            ".job-" + job["verification_id"] + "-" + str(job["owner_epoch"])
        )
        await asyncio.to_thread(self._reset_job_root, job_root)
        archive_path = job_root / "verified.zip"
        try:
            try:
                source, artifact_digest, expected_manifest = await self.validator._source(job)
            except ForestError as exc:
                if exc.code != "BACKUP_RESTORE_UNAVAILABLE":
                    raise
                raise ForestError(
                    "BACKUP_VERIFICATION_UNAVAILABLE", "Backup cannot be verified", 409
                ) from None
            key = base64.urlsafe_b64decode(self.vault.decrypt(job["archive_key_ciphertext"]))
            header = await asyncio.to_thread(decrypt_file_with_key, source, archive_path, key)
            manifest = await asyncio.to_thread(inspect_zip, archive_path)
            manifest_digest = await asyncio.to_thread(
                self.validator._manifest_digest, archive_path
            )
            async with self.store.pool.connection() as conn:
                targets = await (await conn.execute(
                    "SELECT database_scope,bot_id,content_digest,migration_digest,deletion_epoch "
                    "FROM backup_database_cuts WHERE backup_id=%s "
                    "ORDER BY database_scope,bot_id", (job["backup_id"],)
                )).fetchall()
            await self.validator._validate_manifest(
                job, header, manifest, manifest_digest, expected_manifest,
                artifact_digest, targets=targets, publish_restore=False,
            )
            now = datetime.now(UTC)
            expires = now + timedelta(seconds=self.receipt_seconds)
            async with self.store.pool.connection() as conn, conn.transaction():
                row = await (await conn.execute(
                    "UPDATE backup_verification_jobs SET state='VERIFIED',owner_id=NULL,"
                    "lease_expires_at=NULL,manifest_digest=%s,member_total=%s,plaintext_size=%s,"
                    "error_code=NULL,verified_at=%s,expires_at=%s,updated_at=clock_timestamp() "
                    "WHERE verification_id=%s AND state='VERIFYING' AND owner_id=%s "
                    "AND owner_epoch=%s AND lease_expires_at>clock_timestamp() RETURNING *",
                    (manifest_digest, len(manifest["members"]), header["plaintext_size"], now,
                     expires, job["verification_id"], self.owner_id, job["owner_epoch"]),
                )).fetchone()
                if row is None:
                    raise ForestError(
                        "VERIFICATION_LEASE_LOST", "Verification ownership was lost", 409
                    )
                await conn.execute(
                    "INSERT INTO backup_verification_events(verification_id,owner_epoch,step,"
                    "details) VALUES (%s,%s,'VERIFIED',%s)",
                    (job["verification_id"], job["owner_epoch"], Jsonb({
                        "manifest_digest": manifest_digest,
                        "database_total": job["database_total"],
                        "member_total": len(manifest["members"]),
                    })),
                )
                await conn.execute(
                    "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,"
                    "details) VALUES (%s,%s,'backup.verify.completed',%s,%s,%s)",
                    (str(uuid4()), job["subject_id"], "backup:" + job["backup_id"],
                     job["verification_id"], Jsonb({
                         "verification_id": job["verification_id"],
                         "backup_id": job["backup_id"],
                         "manifest_digest": manifest_digest,
                         "database_total": job["database_total"],
                     })),
                )
                return _public(row)
        finally:
            await asyncio.to_thread(self._remove_job_root, job_root)

    async def _read_internal(self, verification_id: str) -> dict[str, Any]:
        async with self.store.pool.connection() as conn:
            row = await (await conn.execute(
                _SELECT + "WHERE verification_id=%s", (verification_id,)
            )).fetchone()
            return _public(row)

    @staticmethod
    async def _now(conn) -> datetime:
        return (await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone())["now"]

    @staticmethod
    def _reset_job_root(path: Path) -> None:
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(mode=0o700, parents=False)

    @staticmethod
    def _remove_job_root(path: Path) -> None:
        shutil.rmtree(path, ignore_errors=True)

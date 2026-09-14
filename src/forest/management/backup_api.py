"""Authenticated backup creation, progress and encrypted artifact download."""

from __future__ import annotations

import asyncio

from fastapi import Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

from forest.errors import ForestError
from forest.security import digest_token, verify_password


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CreateBackup(Body):
    recovery_passphrase: str = Field(min_length=16, max_length=1024)
    current_password: str = Field(min_length=12, max_length=1024)
    include_packages: bool = True


class StageRestore(Body):
    verification_id: str = Field(min_length=1, max_length=128)
    current_password: str = Field(min_length=12, max_length=1024)


class VerifyBackup(Body):
    recovery_passphrase: str = Field(min_length=16, max_length=1024)
    current_password: str = Field(min_length=12, max_length=1024)


class ReserveBackupImport(Body):
    expected_size: int = Field(ge=1, le=68719493184, strict=True)
    current_password: str = Field(min_length=12, max_length=1024)


class FinalizeBackupImport(Body):
    recovery_passphrase: str = Field(min_length=16, max_length=1024)
    current_password: str = Field(min_length=12, max_length=1024)


class CleanupPlan(Body):
    pass


class CleanupApply(Body):
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)
    current_password: str = Field(min_length=12, max_length=1024)


class CleanupRetry(Body):
    current_password: str = Field(min_length=12, max_length=1024)


class ActivationPlan(Body):
    target_ordinal: int = Field(ge=0, le=1023, strict=True)


class ActivationApply(Body):
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)
    current_password: str = Field(min_length=12, max_length=1024)


class ActivationRetry(Body):
    current_password: str = Field(min_length=12, max_length=1024)


def install_backup_routes(app, runtime) -> None:
    def manager():
        if runtime.backups is None:
            raise ForestError("BACKUP_SERVICE_UNAVAILABLE", "Backup service is unavailable", 503)
        return runtime.backups

    def key(request: Request) -> str:
        value = request.headers.get("idempotency-key", "")
        if not 1 <= len(value) <= 128:
            raise ForestError("IDEMPOTENCY_KEY_REQUIRED", "A stable backup key is required", 422)
        return value

    async def reauthenticate(session: dict, password: str) -> str:
        store = runtime.require_store()
        async with store.pool.connection() as conn:
            account = await (await conn.execute(
                "SELECT password_hash,disabled,role FROM web_accounts WHERE id=%s",
                (session["account_id"],),
            )).fetchone()
        encoded = account["password_hash"] if account else "malformed"
        async with runtime.password_slots:
            valid = await asyncio.to_thread(verify_password, password, encoded)
        if (
            not valid or account is None or account["disabled"] or account["role"] != "admin"
        ):
            raise ForestError("REAUTHENTICATION_FAILED", "Current password is incorrect", 403)
        return account["password_hash"]

    @app.get("/management/v1/backups")
    async def backups(request: Request):
        session = await runtime.principal(request, require_admin=True)
        rows = await manager().list(
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )
        return {"backups": rows, "format": "forest.backup.v1"}

    @app.post("/management/v1/backups", status_code=202)
    async def create_backup(body: CreateBackup, request: Request):
        session = await runtime.principal(request, write=True)
        expected_hash = await reauthenticate(session, body.current_password)
        if runtime.authority is None:
            raise ForestError("AUTHORITY_UNAVAILABLE", "Web authority is unavailable", 503)
        return await manager().create(
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
            expected_password_hash=expected_hash,
            passphrase=body.recovery_passphrase,
            passphrase_digest=runtime.authority.secret_digest(
                "backup.recovery-passphrase", body.recovery_passphrase
            ),
            include_packages=body.include_packages,
            idempotency_key=key(request),
        )

    @app.get("/management/v1/backups/{backup_id}")
    async def read_backup(backup_id: str, request: Request):
        session = await runtime.principal(request, require_admin=True)
        return await manager().read(
            backup_id=backup_id,
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )

    @app.get("/management/v1/backups/{backup_id}/download")
    async def download_backup(backup_id: str, request: Request):
        session = await runtime.principal(request, require_admin=True)
        path, metadata = await manager().artifact(
            backup_id=backup_id,
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )
        return FileResponse(
            path,
            media_type="application/vnd.forest.backup",
            filename=f"forest-backup-{backup_id}.bin",
            headers={
                "Cache-Control": "no-store",
                "X-Content-SHA256": metadata["ciphertext_digest"].removeprefix("sha256:"),
            },
        )

    def restores():
        if runtime.restores is None:
            raise ForestError("RESTORE_SERVICE_UNAVAILABLE", "Restore service is unavailable", 503)
        return runtime.restores

    def cleanup():
        if runtime.cleanup is None:
            raise ForestError("CLEANUP_SERVICE_UNAVAILABLE", "Cleanup service is unavailable", 503)
        return runtime.cleanup

    def activations():
        if runtime.activations is None:
            raise ForestError(
                "ACTIVATION_SERVICE_UNAVAILABLE", "Restore activation service is unavailable", 503
            )
        return runtime.activations

    def verifications():
        if runtime.verifications is None:
            raise ForestError(
                "VERIFICATION_SERVICE_UNAVAILABLE", "Verification service is unavailable", 503
            )
        return runtime.verifications

    def imports():
        if runtime.backup_imports is None:
            raise ForestError(
                "BACKUP_IMPORT_SERVICE_UNAVAILABLE", "Backup import service is unavailable", 503
            )
        return runtime.backup_imports

    @app.get("/management/v1/backup-imports")
    async def backup_imports(request: Request):
        session = await runtime.principal(request, require_admin=True)
        rows = await imports().list(
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )
        return {"imports": rows, "format": "forest.backup.v1"}

    @app.post("/management/v1/backup-imports", status_code=201)
    async def reserve_backup_import(body: ReserveBackupImport, request: Request):
        session = await runtime.principal(request, write=True)
        expected_hash = await reauthenticate(session, body.current_password)
        return await imports().reserve(
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
            expected_password_hash=expected_hash, expected_size=body.expected_size,
            idempotency_key=key(request),
        )

    @app.get("/management/v1/backup-imports/{import_id}")
    async def read_backup_import(import_id: str, request: Request):
        session = await runtime.principal(request, require_admin=True)
        return await imports().read(
            import_id=import_id, subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )

    @app.put("/management/v1/backup-imports/{import_id}/artifact")
    async def upload_backup_import(import_id: str, request: Request):
        session = await runtime.principal(
            request, write=True, content_type="application/vnd.forest.backup"
        )
        return await imports().upload(
            import_id=import_id, subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
            chunks=request.stream(),
        )

    @app.post("/management/v1/backup-imports/{import_id}/finalize", status_code=202)
    async def finalize_backup_import(
        import_id: str, body: FinalizeBackupImport, request: Request
    ):
        session = await runtime.principal(request, write=True)
        expected_hash = await reauthenticate(session, body.current_password)
        if runtime.authority is None:
            raise ForestError("AUTHORITY_UNAVAILABLE", "Web authority is unavailable", 503)
        return await imports().queue(
            import_id=import_id, subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
            expected_password_hash=expected_hash, passphrase=body.recovery_passphrase,
            passphrase_digest=runtime.authority.secret_digest(
                "backup.import-passphrase", body.recovery_passphrase
            ),
        )

    @app.get("/management/v1/backup-verifications")
    async def backup_verifications(request: Request):
        session = await runtime.principal(request, require_admin=True)
        rows = await verifications().list(
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )
        return {"verifications": rows}

    @app.post("/management/v1/backups/{backup_id}/verifications", status_code=202)
    async def create_backup_verification(backup_id: str, body: VerifyBackup, request: Request):
        session = await runtime.principal(request, write=True)
        expected_hash = await reauthenticate(session, body.current_password)
        if runtime.authority is None:
            raise ForestError("AUTHORITY_UNAVAILABLE", "Web authority is unavailable", 503)
        return await verifications().create(
            backup_id=backup_id, subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
            expected_password_hash=expected_hash, passphrase=body.recovery_passphrase,
            passphrase_digest=runtime.authority.secret_digest(
                "backup.verification-passphrase", body.recovery_passphrase
            ),
            idempotency_key=key(request),
        )

    @app.get("/management/v1/backup-verifications/{verification_id}")
    async def read_backup_verification(verification_id: str, request: Request):
        session = await runtime.principal(request, require_admin=True)
        return await verifications().read(
            verification_id=verification_id, subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )

    @app.post("/management/v1/backups/{backup_id}/cleanup-plan")
    async def plan_backup_cleanup(backup_id: str, body: CleanupPlan, request: Request):
        session = await runtime.principal(request, write=True)
        return await cleanup().plan(
            kind="BACKUP_ARTIFACT", resource_id=backup_id,
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )

    @app.post("/management/v1/restore-stages/{restore_id}/cleanup-plan")
    async def plan_restore_cleanup(restore_id: str, body: CleanupPlan, request: Request):
        session = await runtime.principal(request, write=True)
        return await cleanup().plan(
            kind="RESTORE_STAGE", resource_id=restore_id,
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )

    @app.post("/management/v1/storage-cleanups", status_code=202)
    async def apply_cleanup(body: CleanupApply, request: Request):
        session = await runtime.principal(request, write=True)
        expected_hash = await reauthenticate(session, body.current_password)
        return await cleanup().apply(
            plan_id=body.plan_id, plan_digest=body.plan_digest,
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
            expected_password_hash=expected_hash, idempotency_key=key(request),
        )

    @app.get("/management/v1/storage-cleanups/{cleanup_id}")
    async def read_cleanup(cleanup_id: str, request: Request):
        session = await runtime.principal(request, require_admin=True)
        return await cleanup().read(
            cleanup_id=cleanup_id, subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )

    @app.post("/management/v1/storage-cleanups/{cleanup_id}/retry", status_code=202)
    async def retry_cleanup(cleanup_id: str, body: CleanupRetry, request: Request):
        session = await runtime.principal(request, write=True)
        expected_hash = await reauthenticate(session, body.current_password)
        return await cleanup().retry(
            cleanup_id=cleanup_id, subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
            expected_password_hash=expected_hash, idempotency_key=key(request),
        )

    @app.get("/management/v1/restore-stages")
    async def restore_stages(request: Request):
        session = await runtime.principal(request, require_admin=True)
        rows = await restores().list(
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )
        return {"restore_stages": rows}

    @app.post("/management/v1/backups/{backup_id}/restore-stages", status_code=202)
    async def create_restore_stage(backup_id: str, body: StageRestore, request: Request):
        session = await runtime.principal(request, write=True)
        expected_hash = await reauthenticate(session, body.current_password)
        if runtime.authority is None:
            raise ForestError("AUTHORITY_UNAVAILABLE", "Web authority is unavailable", 503)
        return await restores().create(
            backup_id=backup_id,
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
            expected_password_hash=expected_hash,
            verification_id=body.verification_id,
            idempotency_key=key(request),
        )

    @app.get("/management/v1/restore-stages/{restore_id}")
    async def read_restore_stage(restore_id: str, request: Request):
        session = await runtime.principal(request, require_admin=True)
        return await restores().read(
            restore_id=restore_id,
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )

    @app.post("/management/v1/restore-stages/{restore_id}/activation-plans")
    async def plan_restore_activation(
        restore_id: str, body: ActivationPlan, request: Request
    ):
        session = await runtime.principal(request, write=True)
        return await activations().plan(
            restore_id=restore_id, target_ordinal=body.target_ordinal,
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )

    @app.get("/management/v1/restore-activations")
    async def restore_activations(request: Request):
        session = await runtime.principal(request, require_admin=True)
        rows = await activations().list(
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )
        return {"restore_activations": rows}

    @app.post("/management/v1/restore-activations", status_code=202)
    async def apply_restore_activation(body: ActivationApply, request: Request):
        session = await runtime.principal(request, write=True)
        expected_hash = await reauthenticate(session, body.current_password)
        return await activations().apply(
            plan_id=body.plan_id, plan_digest=body.plan_digest,
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
            expected_password_hash=expected_hash, idempotency_key=key(request),
        )

    @app.get("/management/v1/restore-activations/{activation_id}")
    async def read_restore_activation(activation_id: str, request: Request):
        session = await runtime.principal(request, require_admin=True)
        return await activations().read(
            activation_id=activation_id, subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )

    @app.post(
        "/management/v1/restore-activations/{activation_id}/retry", status_code=202
    )
    async def retry_restore_activation(
        activation_id: str, body: ActivationRetry, request: Request
    ):
        session = await runtime.principal(request, write=True)
        expected_hash = await reauthenticate(session, body.current_password)
        return await activations().retry(
            activation_id=activation_id, subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
            expected_password_hash=expected_hash, idempotency_key=key(request),
        )

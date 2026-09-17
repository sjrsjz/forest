"""Deployment-owned backup, verification and external archive contract."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from forest.management.backup_imports import MAX_UPLOAD
from forest.management.module_api import (
    ArtifactFileResult,
    ModuleManagementContract,
    ModuleOperation,
)
from forest.management.reauthentication import reauthenticate_admin


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ListQuery(Body):
    limit: int = Field(default=100, ge=1, le=200)


class BackupIdentity(Body):
    backup_id: str = Field(min_length=1, max_length=128)


class ImportIdentity(Body):
    import_id: str = Field(min_length=1, max_length=128)


class VerificationIdentity(Body):
    verification_id: str = Field(min_length=1, max_length=128)


class BackupCreate(Body):
    recovery_passphrase: str = Field(min_length=16, max_length=1024)
    current_password: str = Field(min_length=12, max_length=1024)
    include_packages: bool = True


class ImportReserve(Body):
    expected_size: int = Field(ge=1, le=MAX_UPLOAD, strict=True)
    current_password: str = Field(min_length=12, max_length=1024)


class ImportFinalize(ImportIdentity):
    recovery_passphrase: str = Field(min_length=16, max_length=1024)
    current_password: str = Field(min_length=12, max_length=1024)


class VerificationCreate(BackupIdentity):
    recovery_passphrase: str = Field(min_length=16, max_length=1024)
    current_password: str = Field(min_length=12, max_length=1024)


class DeploymentBackupManagementContract(ModuleManagementContract):
    def __init__(self, runtime):
        self.runtime = runtime
        super().__init__(
            (
                ModuleOperation(
                    "backups", "query", "deployment.backup.read", ListQuery,
                    self.backups, principal="admin",
                ),
                ModuleOperation(
                    "backup", "query", "deployment.backup.read", BackupIdentity,
                    self.backup, principal="admin",
                ),
                ModuleOperation(
                    "backup.create", "action", "deployment.backup.create", BackupCreate,
                    self.backup_create, authorization_preparer=self._prepare_backup_create,
                    principal="admin", success_status=202,
                ),
                ModuleOperation(
                    "backup.archive", "download", "deployment.backup.read", BackupIdentity,
                    self.backup_archive, principal="admin",
                    media_type="application/vnd.forest.backup", max_bytes=MAX_UPLOAD,
                    disposition="attachment",
                ),
                ModuleOperation(
                    "imports", "query", "deployment.backup.import.read", ListQuery,
                    self.imports, principal="admin",
                ),
                ModuleOperation(
                    "import", "query", "deployment.backup.import.read", ImportIdentity,
                    self.backup_import, principal="admin",
                ),
                ModuleOperation(
                    "import.reserve", "action", "deployment.backup.import.reserve",
                    ImportReserve, self.import_reserve,
                    authorization_preparer=self._prepare_import_reserve,
                    principal="admin", success_status=201,
                ),
                ModuleOperation(
                    "import.artifact", "upload", "deployment.backup.import.upload",
                    ImportIdentity, self.import_artifact, principal="admin",
                    media_type="application/vnd.forest.backup", max_bytes=MAX_UPLOAD,
                    artifact_mode="stream", artifact_timeout_seconds=3600,
                ),
                ModuleOperation(
                    "import.finalize", "action", "deployment.backup.import.finalize",
                    ImportFinalize, self.import_finalize,
                    authorization_preparer=self._prepare_import_finalize,
                    principal="admin", success_status=202,
                ),
                ModuleOperation(
                    "verifications", "query", "deployment.backup.verify.read", ListQuery,
                    self.verifications, principal="admin",
                ),
                ModuleOperation(
                    "verification", "query", "deployment.backup.verify.read",
                    VerificationIdentity, self.verification, principal="admin",
                ),
                ModuleOperation(
                    "verification.create", "action", "deployment.backup.verify.create",
                    VerificationCreate, self.verification_create,
                    authorization_preparer=self._prepare_verification_create,
                    principal="admin", success_status=202,
                ),
            )
        )

    async def _reauthenticate(self, request, password: str) -> str:
        return await reauthenticate_admin(
            self.runtime.store,
            self.runtime.password_slots,
            request.principal["subject_id"],
            password,
        )

    async def _prepare_secret(self, request, password, passphrase, purpose, payload):
        expected_hash = await self._reauthenticate(request, password)
        digest = self.runtime.authority.secret_digest(purpose, passphrase)
        return {**payload, "passphrase_digest": digest}, (expected_hash, passphrase, digest)

    async def _prepare_backup_create(self, request, body):
        return await self._prepare_secret(
            request, body.current_password, body.recovery_passphrase,
            "backup.recovery-passphrase", {"include_packages": body.include_packages},
        )

    async def _prepare_import_reserve(self, request, body):
        expected_hash = await self._reauthenticate(request, body.current_password)
        return {"expected_size": body.expected_size}, expected_hash

    async def _prepare_import_finalize(self, request, body):
        return await self._prepare_secret(
            request, body.current_password, body.recovery_passphrase,
            "backup.import-passphrase", {"import_id": body.import_id},
        )

    async def _prepare_verification_create(self, request, body):
        return await self._prepare_secret(
            request, body.current_password, body.recovery_passphrase,
            "backup.verification-passphrase", {"backup_id": body.backup_id},
        )

    @staticmethod
    def _principal(request):
        return {
            "subject_id": request.principal["subject_id"],
            "session_token_hash": request.principal["session_token_hash"],
        }

    async def backups(self, request, body):
        rows = await self.runtime.backups.list(**self._principal(request), limit=body.limit)
        return {"backups": rows, "format": "forest.backup.v1"}

    async def backup(self, request, body):
        return await self.runtime.backups.read(
            backup_id=body.backup_id, **self._principal(request)
        )

    async def backup_create(self, request, body):
        expected_hash, passphrase, digest = request.prepared
        return await self.runtime.backups.create(
            **self._principal(request), expected_password_hash=expected_hash,
            passphrase=passphrase, passphrase_digest=digest,
            include_packages=body.include_packages, idempotency_key=request.operation_id,
        )

    async def backup_archive(self, request, body):
        path, metadata = await self.runtime.backups.artifact(
            backup_id=body.backup_id, **self._principal(request)
        )
        return ArtifactFileResult(
            path, metadata["byte_size"], f"forest-backup-{body.backup_id}.bin"
        )

    async def imports(self, request, body):
        rows = await self.runtime.backup_imports.list(
            **self._principal(request), limit=body.limit
        )
        return {"imports": rows, "format": "forest.backup.v1"}

    async def backup_import(self, request, body):
        return await self.runtime.backup_imports.read(
            import_id=body.import_id, **self._principal(request)
        )

    async def import_reserve(self, request, body):
        return await self.runtime.backup_imports.reserve(
            **self._principal(request), expected_password_hash=request.prepared,
            expected_size=body.expected_size, idempotency_key=request.operation_id,
        )

    async def import_artifact(self, request, body):
        return await self.runtime.backup_imports.upload(
            import_id=body.import_id, **self._principal(request), chunks=request.artifact
        )

    async def import_finalize(self, request, body):
        expected_hash, passphrase, digest = request.prepared
        return await self.runtime.backup_imports.queue(
            import_id=body.import_id, **self._principal(request),
            expected_password_hash=expected_hash, passphrase=passphrase,
            passphrase_digest=digest,
        )

    async def verifications(self, request, body):
        rows = await self.runtime.verifications.list(
            **self._principal(request), limit=body.limit
        )
        return {"verifications": rows}

    async def verification(self, request, body):
        return await self.runtime.verifications.read(
            verification_id=body.verification_id, **self._principal(request)
        )

    async def verification_create(self, request, body):
        expected_hash, passphrase, digest = request.prepared
        return await self.runtime.verifications.create(
            backup_id=body.backup_id, **self._principal(request),
            expected_password_hash=expected_hash, passphrase=passphrase,
            passphrase_digest=digest, idempotency_key=request.operation_id,
        )

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

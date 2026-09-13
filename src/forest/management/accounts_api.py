"""HTTP account administration backed by deployment-authority transactions."""

from __future__ import annotations

import asyncio
import secrets
from datetime import UTC, datetime, timedelta
from typing import Literal

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from forest.errors import ForestError
from forest.management.accounts import AccountManager
from forest.security import digest_token, hash_password, verify_password


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CreateAccount(Body):
    username: str = Field(min_length=1, max_length=80, pattern=r"^[a-zA-Z0-9_.-]+$")
    password: str = Field(min_length=12, max_length=1024)
    role: Literal["admin", "viewer"]
    current_password: str = Field(min_length=12, max_length=1024)


class AccountPlan(Body):
    expected_revision: int = Field(ge=1, le=2**63 - 1, strict=True)
    role: Literal["admin", "viewer"]
    disabled: bool = Field(strict=True)
    new_password: str | None = Field(default=None, min_length=12, max_length=1024)


class AccountApply(Body):
    expected_revision: int = Field(ge=1, le=2**63 - 1, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)
    current_password: str = Field(min_length=12, max_length=1024)


def install_account_routes(app: FastAPI, runtime) -> None:
    def manager() -> AccountManager:
        if runtime.authority is None:
            raise ForestError("AUTHORITY_UNAVAILABLE", "Web authority is unavailable", 503)
        return AccountManager(runtime.require_store(), runtime.authority.vault)

    def key(request: Request) -> str:
        value = request.headers.get("idempotency-key", "")
        if not 1 <= len(value) <= 128:
            raise ForestError("IDEMPOTENCY_KEY_REQUIRED", "A stable idempotency key is required", 422)
        return value

    async def reauthenticate(
        session: dict, password: str, *, require_admin: bool = True
    ) -> str:
        account = await manager().credentials(session["account_id"])
        # Retain comparable scrypt work even if a concurrent delete/corrupt state
        # makes the session subject unavailable. Accounts are normally never deleted.
        encoded = account["password_hash"] if account else "malformed"
        async with runtime.password_slots:
            valid = await asyncio.to_thread(verify_password, password, encoded)
        if (
            not valid
            or account is None
            or account["disabled"]
            or require_admin
            and account["role"] != "admin"
        ):
            raise ForestError("REAUTHENTICATION_FAILED", "Current password is incorrect", 403)
        return account["password_hash"]

    async def password_material(password: str, *, purpose: str) -> tuple[str, str]:
        async with runtime.password_slots:
            password_hash = await asyncio.to_thread(hash_password, password)
        if runtime.authority is None:
            raise ForestError("AUTHORITY_UNAVAILABLE", "Web authority is unavailable", 503)
        return password_hash, runtime.authority.secret_digest(purpose, password)

    @app.get("/management/v1/accounts")
    async def accounts(request: Request):
        session = await runtime.principal(request, require_admin=True)
        rows = await manager().list(
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
        )
        return {"accounts": rows, "roles": ["admin", "viewer"]}

    @app.post("/management/v1/accounts", status_code=201)
    async def create(body: CreateAccount, request: Request):
        session = await runtime.principal(request, write=True)
        current_hash = await reauthenticate(session, body.current_password)
        password_hash, secret = await password_material(
            body.password, purpose="account.create.password"
        )
        return await manager().create(
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
            expected_password_hash=current_hash,
            username=body.username,
            password_hash=password_hash,
            role=body.role,
            secret_digest=secret,
            idempotency_key=key(request),
        )

    @app.post("/management/v1/accounts/{account_id}/change-plans")
    async def plan(account_id: str, body: AccountPlan, request: Request):
        session = await runtime.principal(request, write=True)
        password_hash_ciphertext = password_secret_digest = None
        if body.new_password is not None:
            password_hash, password_secret_digest = await password_material(
                body.new_password, purpose="account.change.password"
            )
            password_hash_ciphertext = runtime.authority.vault.encrypt(password_hash)
        return await manager().plan(
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
            target_account_id=account_id,
            expected_revision=body.expected_revision,
            role=body.role,
            disabled=body.disabled,
            password_hash_ciphertext=password_hash_ciphertext,
            password_secret_digest=password_secret_digest,
            plan_id="account-plan-" + secrets.token_hex(16),
            expires_at=datetime.now(UTC) + timedelta(minutes=10),
        )

    @app.post("/management/v1/accounts/{account_id}/management-requests")
    async def apply(account_id: str, body: AccountApply, request: Request):
        # A subject whose own committed change demoted it may recover the exact
        # idempotent receipt after logging in again. AccountManager still rejects
        # every new operation unless the transaction sees a current administrator.
        session = await runtime.principal(request, write=True, require_admin=False)
        current_hash = await reauthenticate(
            session, body.current_password, require_admin=False
        )
        result = await manager().apply(
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
            expected_password_hash=current_hash,
            target_account_id=account_id,
            expected_revision=body.expected_revision,
            plan_id=body.plan_id,
            plan_digest=body.plan_digest,
            idempotency_key=key(request),
        )
        response = JSONResponse(result)
        # The original self-change revoked the session used to commit it. A
        # later idempotent recovery may use a newly established session and
        # must not orphan that fresh session by deleting only its browser cookie.
        if result["reauthentication_required"]:
            session_still_live = await runtime.require_store().get_session(
                digest_token(request.cookies["forest_session"])
            )
            if session_still_live is None:
                response.delete_cookie("forest_session", path="/")
                response.delete_cookie("forest_csrf", path="/")
        return response

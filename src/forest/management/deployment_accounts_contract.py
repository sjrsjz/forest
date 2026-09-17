"""Deployment-owned Web account management contract."""

from __future__ import annotations

import asyncio
import secrets
from datetime import UTC, datetime, timedelta
from typing import Literal

from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from forest.errors import ForestError
from forest.management.module_api import ModuleManagementContract, ModuleOperation
from forest.security import hash_password, verify_password


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CreateAccount(Body):
    username: str = Field(min_length=1, max_length=80, pattern=r"^[a-zA-Z0-9_.-]+$")
    password: str = Field(min_length=12, max_length=1024)
    role: Literal["admin", "viewer"]
    current_password: str = Field(min_length=12, max_length=1024)


class AccountPlan(Body):
    account_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=1, le=2**63 - 1, strict=True)
    role: Literal["admin", "viewer"]
    disabled: bool = Field(strict=True)
    new_password: str | None = Field(default=None, min_length=12, max_length=1024)


class AccountApply(Body):
    account_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=1, le=2**63 - 1, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)
    current_password: str = Field(min_length=12, max_length=1024)


def _create_payload(body):
    return {"username": body.username, "role": body.role, "credentials_changed": True}


def _plan_payload(body):
    return {
        "account_id": body.account_id,
        "expected_revision": body.expected_revision,
        "role": body.role,
        "disabled": body.disabled,
        "credentials_changed": body.new_password is not None,
    }


def _apply_payload(body):
    return body.model_dump(exclude={"current_password"})


class DeploymentAccountsManagementContract(ModuleManagementContract):
    def __init__(self, runtime):
        self.runtime = runtime
        super().__init__(
            (
                ModuleOperation(
                    "accounts", "query", "deployment.accounts.read", Body,
                    self.accounts, principal="admin",
                ),
                ModuleOperation(
                    "account.create", "action", "deployment.accounts.create", CreateAccount,
                    self.create, authorization_payload=_create_payload, principal="admin",
                ),
                ModuleOperation(
                    "account.change.plan", "action", "deployment.accounts.change.plan",
                    AccountPlan, self.plan, authorization_payload=_plan_payload,
                    principal="admin",
                ),
                ModuleOperation(
                    "account.change.apply", "action", "deployment.accounts.change.apply",
                    AccountApply, self.apply, authorization_payload=_apply_payload,
                ),
            )
        )

    async def _reauthenticate(self, request, password, *, require_admin=True):
        account = await self.runtime.manager.credentials(request.principal["subject_id"])
        encoded = account["password_hash"] if account else "malformed"
        async with self.runtime.password_slots:
            valid = await asyncio.to_thread(verify_password, password, encoded)
        if (
            not valid
            or account is None
            or account["disabled"]
            or require_admin and account["role"] != "admin"
        ):
            raise ForestError("REAUTHENTICATION_FAILED", "Current password is incorrect", 403)
        return account["password_hash"]

    async def _password_material(self, password, *, purpose):
        async with self.runtime.password_slots:
            password_hash = await asyncio.to_thread(hash_password, password)
        return password_hash, self.runtime.authority.secret_digest(purpose, password)

    async def accounts(self, request, body):
        rows = await self.runtime.manager.list(
            subject_id=request.principal["subject_id"],
            session_token_hash=request.principal["session_token_hash"],
        )
        return {"accounts": rows, "roles": ["admin", "viewer"]}

    async def create(self, request, body):
        current_hash = await self._reauthenticate(request, body.current_password)
        password_hash, secret = await self._password_material(
            body.password, purpose="account.create.password"
        )
        result = await self.runtime.manager.create(
            subject_id=request.principal["subject_id"],
            session_token_hash=request.principal["session_token_hash"],
            expected_password_hash=current_hash,
            username=body.username,
            password_hash=password_hash,
            role=body.role,
            secret_digest=secret,
            idempotency_key=request.operation_id,
        )
        return JSONResponse(result, status_code=201)

    async def plan(self, request, body):
        password_hash_ciphertext = password_secret_digest = None
        if body.new_password is not None:
            password_hash, password_secret_digest = await self._password_material(
                body.new_password, purpose="account.change.password"
            )
            password_hash_ciphertext = self.runtime.authority.vault.encrypt(password_hash)
        return await self.runtime.manager.plan(
            subject_id=request.principal["subject_id"],
            session_token_hash=request.principal["session_token_hash"],
            target_account_id=body.account_id,
            expected_revision=body.expected_revision,
            role=body.role,
            disabled=body.disabled,
            password_hash_ciphertext=password_hash_ciphertext,
            password_secret_digest=password_secret_digest,
            plan_id="account-plan-" + secrets.token_hex(16),
            expires_at=datetime.now(UTC) + timedelta(minutes=10),
        )

    async def apply(self, request, body):
        current_hash = await self._reauthenticate(
            request, body.current_password, require_admin=False
        )
        result = await self.runtime.manager.apply(
            subject_id=request.principal["subject_id"],
            session_token_hash=request.principal["session_token_hash"],
            expected_password_hash=current_hash,
            target_account_id=body.account_id,
            expected_revision=body.expected_revision,
            plan_id=body.plan_id,
            plan_digest=body.plan_digest,
            idempotency_key=request.operation_id,
        )
        response = JSONResponse(result)
        if result["reauthentication_required"]:
            session_still_live = await self.runtime.store.get_session(
                request.principal["session_token_hash"]
            )
            if session_still_live is None:
                response.delete_cookie("forest_session", path="/")
                response.delete_cookie("forest_csrf", path="/")
        return response

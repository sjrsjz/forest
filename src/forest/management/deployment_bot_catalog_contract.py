"""Deployment-owned Bot directory and provisioning management contract."""

from __future__ import annotations

from typing import Literal

from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from forest.management.module_api import ModuleManagementContract, ModuleOperation


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BotIdentity(Body):
    bot_id: str = Field(min_length=1, max_length=128)


class JobIdentity(Body):
    job_id: str = Field(min_length=1, max_length=128)


class CreateBot(Body):
    display_name: str = Field(min_length=1, max_length=120)
    mode: Literal["managed", "external"] = "managed"
    dsn: str | None = Field(default=None, max_length=4096)
    expected_revision: Literal[0] = 0


class RenameBot(BotIdentity):
    expected_revision: int = Field(ge=0, strict=True)
    display_name: str = Field(min_length=1, max_length=120)


class RetryBot(BotIdentity):
    expected_revision: int = Field(ge=0, strict=True)


def _create_payload(body):
    return {
        "display_name": body.display_name,
        "mode": body.mode,
        "external_storage": body.dsn is not None,
        "expected_revision": body.expected_revision,
    }


class DeploymentBotCatalogManagementContract(ModuleManagementContract):
    def __init__(self, runtime):
        self.runtime = runtime
        super().__init__(
            (
                ModuleOperation("bots", "query", "deployment.bots.read", Body, self.bots),
                ModuleOperation("bot", "query", "deployment.bots.read", BotIdentity, self.bot),
                ModuleOperation(
                    "provision-job", "query", "deployment.bots.read", JobIdentity,
                    self.provision_job,
                ),
                ModuleOperation(
                    "bot.create", "action", "deployment.bots.create", CreateBot, self.create,
                    authorization_payload=_create_payload, principal="admin",
                ),
                ModuleOperation(
                    "bot.rename", "action", "deployment.bots.rename", RenameBot, self.rename,
                    principal="admin",
                ),
                ModuleOperation(
                    "bot.retry", "action", "deployment.bots.retry", RetryBot, self.retry,
                    principal="admin",
                ),
            )
        )

    def _who(self, request):
        return {
            "subject_id": request.principal["subject_id"],
            "session_token_hash": request.principal["session_token_hash"],
        }

    async def bots(self, request, body):
        rows = await self.runtime.catalog.list(**self._who(request))
        return {
            "bots": [
                {**row, "runtime": self.runtime.fleet.status(row["bot_id"])} for row in rows
            ],
            "managed_available": self.runtime.managed_available,
        }

    async def bot(self, request, body):
        row = await self.runtime.catalog.get(bot_id=body.bot_id, **self._who(request))
        return {**row, "runtime": self.runtime.fleet.status(body.bot_id)}

    async def provision_job(self, request, body):
        return await self.runtime.catalog.read_job(
            job_id=body.job_id, **self._who(request)
        )

    async def create(self, request, body):
        result = await self.runtime.catalog.create(
            **body.model_dump(), **self._who(request), idempotency_key=request.operation_id
        )
        return JSONResponse(result, status_code=202)

    async def rename(self, request, body):
        return await self.runtime.catalog.rename(
            **body.model_dump(), **self._who(request), idempotency_key=request.operation_id
        )

    async def retry(self, request, body):
        result = await self.runtime.catalog.retry(
            **body.model_dump(), **self._who(request), idempotency_key=request.operation_id
        )
        return JSONResponse(result, status_code=202)

"""Module-owned management contract for durable conversation Jobs."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from forest.management.module_api import ModuleManagementContract, ModuleOperation
from forest.schedules import JobSettings


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class JobQuery(Body):
    job_id: str | None = Field(default=None, min_length=1, max_length=128)


class Revision(Body):
    job_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=0, strict=True)


class Plan(Revision):
    values: JobSettings


class Apply(Revision):
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)


class Cancel(Revision):
    scope: Literal["future", "pending", "all"]
    reason: str = Field(min_length=1, max_length=256)


class Replay(Body):
    occurrence_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=1, strict=True)


class JobManagementContract(ModuleManagementContract):
    def __init__(self):
        super().__init__((
            ModuleOperation("definitions", "query", "job.read", JobQuery, self.read),
            ModuleOperation("definition.plan", "action", "job.plan", Plan, self.plan),
            ModuleOperation("definition.apply", "action", "job.apply", Apply, self.apply),
            ModuleOperation("occurrence.run", "action", "job.run", Revision, self.run),
            ModuleOperation("definition.cancel", "action", "job.cancel", Cancel, self.cancel),
            ModuleOperation("occurrence.replay", "action", "job.replay", Replay, self.replay),
        ))

    async def read(self, request, body):
        return await request.module.store.read(
            request.ticket, request.operation_id, request.payload
        )

    async def plan(self, request, body):
        return await request.module.store.plan(
            request.ticket, request.operation_id, request.payload
        )

    @staticmethod
    def _changed(request, result):
        if result is not None and not request.bot.control.is_unavailable(result):
            request.module.notify_change()
        return result

    async def apply(self, request, body):
        return self._changed(
            request,
            await request.module.store.apply(
                request.ticket, request.operation_id, request.payload
            ),
        )

    async def run(self, request, body):
        return self._changed(
            request,
            await request.module.store.run_now(
                request.ticket, request.operation_id, request.payload
            ),
        )

    async def cancel(self, request, body):
        return self._changed(
            request,
            await request.module.store.cancel(
                request.ticket, request.operation_id, request.payload
            ),
        )

    async def replay(self, request, body):
        return self._changed(
            request,
            await request.module.store.replay(
                request.ticket, request.operation_id, request.payload
            ),
        )

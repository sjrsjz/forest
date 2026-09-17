"""Module-owned management contract for the authoritative RouteGraph."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from forest.management.module_api import EmptyInput, ModuleManagementContract, ModuleOperation


class GraphVersionInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    graph_digest: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")


class DispatchTraceInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    conversation_id: str | None = Field(default=None, min_length=1, max_length=256)
    binding_id: str | None = Field(default=None, min_length=1, max_length=128)
    channel_id: str | None = Field(default=None, min_length=1, max_length=256)
    event_id: str | None = Field(default=None, min_length=1, max_length=256)
    component_id: str | None = Field(default=None, min_length=1, max_length=128)
    generation_id: str | None = Field(default=None, min_length=1, max_length=128)
    status: str | None = Field(default=None, min_length=1, max_length=64)
    received_after: str | None = Field(default=None, min_length=1, max_length=64)
    received_before: str | None = Field(default=None, min_length=1, max_length=64)
    before: str | None = Field(default=None, min_length=1, max_length=512, pattern=r"^[A-Za-z0-9_-]+$")
    limit: int = Field(default=50, ge=1, le=100)

    @field_validator(
        "conversation_id", "binding_id", "channel_id", "event_id", "component_id",
        "generation_id", "status",
    )
    @classmethod
    def safe_identifier(cls, value):
        if value is not None and any(ord(char) < 32 or ord(char) == 127 for char in value):
            raise ValueError("control characters are forbidden")
        return value

    @field_validator("received_after", "received_before")
    @classmethod
    def timestamp_with_zone(cls, value):
        if value is not None:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("timestamp must include a UTC offset")
        return value


DeliveryStatus = Literal["PENDING", "INFLIGHT", "RETRY_WAIT", "ACKED", "DEAD", "RETIRED"]


class EventReadInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    subscriber_id: str | None = Field(default=None, min_length=1, max_length=128)
    topic: str | None = Field(default=None, min_length=1, max_length=128)
    status: DeliveryStatus | None = None
    limit: int = Field(default=100, ge=1, le=200)


class EventReplayInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    delivery_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=0, strict=True)


class EventRetirementPlanInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    subscriber_id: str = Field(min_length=1, max_length=128)
    contract_major: int = Field(ge=1, le=2**31 - 1, strict=True)
    expected_revision: int = Field(ge=0, strict=True)


class RetirementApplyInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_revision: int = Field(ge=0, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)


class ComponentJobReadInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    component_id: str | None = Field(default=None, min_length=1, max_length=128)
    handler_id: str | None = Field(default=None, min_length=1, max_length=128)
    status: DeliveryStatus | None = None
    limit: int = Field(default=100, ge=1, le=200)


class ComponentJobReplayInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    job_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=0, strict=True)


class ComponentJobRetirementPlanInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    component_id: str = Field(min_length=1, max_length=128)
    handler_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=0, strict=True)

class RoutingManagementContract(ModuleManagementContract):
    def __init__(self):
        super().__init__((
            ModuleOperation("snapshot", "query", "module.read", EmptyInput, self.snapshot),
            ModuleOperation(
                "snapshot-version", "query", "module.read", GraphVersionInput,
                self.snapshot_version,
            ),
            ModuleOperation(
                "dispatch-traces", "query", "module.read", DispatchTraceInput,
                self.dispatch_traces,
            ),
            ModuleOperation("events", "query", "event.read", EventReadInput, self.events),
            ModuleOperation(
                "event.replay", "action", "event.replay", EventReplayInput,
                self.replay_event,
            ),
            ModuleOperation(
                "event.retirement.plan", "action", "event.retire.plan",
                EventRetirementPlanInput, self.plan_event_retirement,
            ),
            ModuleOperation(
                "event.retirement.apply", "action", "event.retire.apply",
                RetirementApplyInput, self.apply_event_retirement,
            ),
            ModuleOperation(
                "component-jobs", "query", "component_job.read", ComponentJobReadInput,
                self.component_jobs,
            ),
            ModuleOperation(
                "component-job.replay", "action", "component_job.replay",
                ComponentJobReplayInput, self.replay_component_job,
            ),
            ModuleOperation(
                "component-job.retirement.plan", "action", "component_job.retire.plan",
                ComponentJobRetirementPlanInput, self.plan_component_job_retirement,
            ),
            ModuleOperation(
                "component-job.retirement.apply", "action", "component_job.retire.apply",
                RetirementApplyInput, self.apply_component_job_retirement,
            ),
        ))

    async def snapshot(self, request, body):
        return await request.module.graph.read(request.ticket, request.operation_id)

    async def snapshot_version(self, request, body):
        return await request.module.graph.read_version(
            request.ticket, request.operation_id, body.graph_digest
        )

    async def dispatch_traces(self, request, body):
        return await request.module.traces.read(
            request.ticket, request.operation_id, body.model_dump(exclude_none=True)
        )

    async def events(self, request, body):
        return await request.module.events.read(
            request.ticket, request.operation_id, body.model_dump(exclude_none=True)
        )

    async def replay_event(self, request, body):
        result = await request.module.events.replay(
            request.ticket, request.operation_id, body.model_dump()
        )
        request.bot.kernel.notify_events()
        return result

    async def plan_event_retirement(self, request, body):
        return await request.module.events.plan_retirement(
            request.ticket, request.operation_id, body.model_dump()
        )

    async def apply_event_retirement(self, request, body):
        result = await request.module.events.apply_retirement(
            request.ticket, request.operation_id, body.model_dump()
        )
        request.bot.kernel.notify_events()
        return result

    async def component_jobs(self, request, body):
        return await request.module.component_jobs.read(
            request.ticket, request.operation_id, body.model_dump(exclude_none=True)
        )

    async def replay_component_job(self, request, body):
        result = await request.module.component_jobs.replay(
            request.ticket, request.operation_id, body.model_dump()
        )
        request.bot.kernel.notify_component_jobs()
        return result

    async def plan_component_job_retirement(self, request, body):
        return await request.module.component_jobs.plan_retirement(
            request.ticket, request.operation_id, body.model_dump()
        )

    async def apply_component_job_retirement(self, request, body):
        result = await request.module.component_jobs.apply_retirement(
            request.ticket, request.operation_id, body.model_dump()
        )
        request.bot.kernel.notify_component_jobs()
        return result

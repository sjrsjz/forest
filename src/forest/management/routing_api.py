"""Module-owned management contract for the authoritative RouteGraph."""

from datetime import datetime

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

"""Public, bounded per-Bot Agent configuration; credentials are stored separately."""

from copy import deepcopy
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ToolSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_-]{0,63}$")
    component_id: str = Field(min_length=1, max_length=128)
    service: str = Field(min_length=1, max_length=128)
    contract_major: int = Field(default=1, ge=1, le=1000, strict=True)
    description: str = Field(default="", max_length=1000)
    authority: str = Field(default="binding_admin", pattern=r"^(trigger|binding_admin)$")
    approval: str = Field(default="always", pattern=r"^(always|never)$")
    effect: str = Field(default="non_idempotent", pattern=r"^(read|idempotent|non_idempotent)$")


class AgentSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = False
    endpoint: str = Field(default="http://127.0.0.1:8000/v1", max_length=2048)
    model: str = Field(default="", max_length=200)
    preset_context: list[str] = Field(
        default_factory=lambda: ["你是群聊中的助手。根据可靠的作者与群来源理解对话。"],
        max_length=32,
    )
    vision: bool = False
    tool_calls: bool = True
    stream: bool = True
    stream_usage: bool = True
    output_token_parameter: str = Field(
        default="max_tokens", pattern=r"^(max_tokens|max_completion_tokens)$"
    )
    image_token_upper_bound: int = Field(default=4096, ge=1, le=262144, strict=True)
    context_tokens: int = Field(default=32768, ge=1024, le=262144, strict=True)
    max_output_tokens: int = Field(default=2048, ge=64, le=32768, strict=True)
    max_steps: int = Field(default=12, ge=1, le=64, strict=True)
    run_timeout: int = Field(default=300, ge=10, le=3600, strict=True)
    model_timeout: int = Field(default=90, ge=1, le=300, strict=True)
    daily_token_budget: int = Field(default=200000, ge=1024, le=100000000, strict=True)
    run_token_budget: int = Field(default=32768, ge=1024, le=1000000, strict=True)
    max_tool_iterations: int = Field(default=8, ge=0, le=32, strict=True)
    max_thinking_iterations: int = Field(default=4, ge=0, le=16, strict=True)
    max_protocol_repairs: int = Field(default=1, ge=0, le=1, strict=True)
    protocol_candidate_byte_limit: int = Field(
        default=1024 * 1024, ge=4096, le=4 * 1024 * 1024, strict=True
    )
    protocol_repair_feedback_byte_limit: int = Field(
        default=8192, ge=1024, le=32768, strict=True
    )
    allowed_tools: list[ToolSettings] = Field(default_factory=list, max_length=16)
    media_allowed_origins: list[str] = Field(default_factory=list, max_length=32)

    @field_validator("endpoint")
    @classmethod
    def endpoint_url(cls, value):
        try:
            parts = urlsplit(value)
            if (
                parts.scheme not in {"http", "https"}
                or not parts.hostname
                or parts.username is not None
                or parts.password is not None
                or parts.query
                or parts.fragment
                or "\\" in value
                or any(ord(c) < 33 or ord(c) == 127 for c in value)
                or (parts.port is not None and not 1 <= parts.port <= 65535)
            ):
                raise ValueError()
        except ValueError:
            raise ValueError(
                "Use an HTTP model base URL without embedded credentials or query"
            ) from None
        return value.rstrip("/")

    @field_validator("model")
    @classmethod
    def model_identity(cls, value):
        if any(ord(c) < 32 or ord(c) == 127 for c in value):
            raise ValueError("Model names cannot contain control characters")
        return value

    @field_validator("preset_context")
    @classmethod
    def prompt_bytes(cls, value):
        if any(not item.strip() for item in value):
            raise ValueError("PresetContext messages cannot be empty")
        if sum(len(item.encode("utf-8")) for item in value) > 32768:
            raise ValueError("PresetContext must fit the 32768-byte model input limit")
        return value

    @field_validator("media_allowed_origins")
    @classmethod
    def origins(cls, values):
        result = []
        for value in values:
            endpoint = cls.endpoint_url(value)
            if urlsplit(endpoint).path:
                raise ValueError("Media permissions require exact HTTP origins without paths")
            result.append(endpoint)
        return sorted(set(result))

    @model_validator(mode="after")
    def coherent(self):
        if self.enabled and not self.model.strip():
            raise ValueError("An enabled Agent requires an explicit model name")
        if self.max_output_tokens >= self.context_tokens:
            raise ValueError("Output budget must leave room for input")
        if self.model_timeout > self.run_timeout:
            raise ValueError("Model timeout cannot exceed the whole Run deadline")
        if self.allowed_tools and not self.tool_calls:
            raise ValueError("Enable model tool calls before configuring tools")
        if any(tool.name == "forest_analyze" for tool in self.allowed_tools):
            raise ValueError("forest_analyze is a retired product tool name")
        if len({tool.name for tool in self.allowed_tools}) != len(self.allowed_tools):
            raise ValueError("Tool names must be unique")
        return self


DEFAULT_AGENT_SETTINGS = AgentSettings().model_dump()

_RETIRED_FIELDS = {
    "children_enabled",
    "max_children",
    "child_concurrency",
    "child_timeout",
    "child_max_output_tokens",
}


def project_stored_agent_values(values: dict) -> dict:
    """Project legacy field names/defaults without changing its receipt body."""
    if not isinstance(values, dict):
        raise ValueError("Stored Agent settings must be an object")
    result = deepcopy(values)
    legacy_prompt = result.pop("system_prompt", None)
    for field in _RETIRED_FIELDS:
        result.pop(field, None)
    if "preset_context" not in result:
        result["preset_context"] = [legacy_prompt] if legacy_prompt else []
    for key, value in DEFAULT_AGENT_SETTINGS.items():
        result.setdefault(key, deepcopy(value))
    return result


def normalize_stored_agent_values(values: dict) -> dict:
    """Validate an immutable legacy config body against the public runtime schema.

    The payload itself is never rewritten.  A subsequent administrator apply
    persists the normalized schema as a new revision.
    """
    result = project_stored_agent_values(values)
    try:
        return AgentSettings.model_validate(result).model_dump()
    except ValueError as exc:
        raise ValueError("Stored Agent settings cannot be normalized") from exc

"""Public, bounded per-Bot Agent configuration; credentials are stored separately."""

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
    system_prompt: str = Field(
        default="你是群聊中的助手。根据可靠的作者与群来源理解对话。", max_length=16000
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
    children_enabled: bool = False
    max_children: int = Field(default=4, ge=1, le=8, strict=True)
    child_concurrency: int = Field(default=2, ge=1, le=2, strict=True)
    child_timeout: int = Field(default=90, ge=1, le=300, strict=True)
    child_max_output_tokens: int = Field(default=1024, ge=64, le=8192, strict=True)
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

    @field_validator("system_prompt")
    @classmethod
    def prompt_bytes(cls, value):
        if len(value.encode("utf-8")) > 32768:
            raise ValueError("System prompt must fit the 32768-byte model input limit")
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
        if self.children_enabled and not self.tool_calls:
            raise ValueError("Private analysis requires parent model tool calls")
        if self.children_enabled and len(self.allowed_tools) > 15:
            raise ValueError("Private analysis reserves one of the sixteen model tool definitions")
        if any(tool.name == "forest_analyze" for tool in self.allowed_tools):
            raise ValueError("forest_analyze is a reserved host tool")
        if len({tool.name for tool in self.allowed_tools}) != len(self.allowed_tools):
            raise ValueError("Tool names must be unique")
        return self


DEFAULT_AGENT_SETTINGS = AgentSettings().model_dump()

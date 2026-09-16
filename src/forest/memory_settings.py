"""Public native multimodal memory configuration; no credentials or inferred claims."""

from typing import Literal
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator


class MemorySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = Field(default=False, strict=True)
    semantic_associations_enabled: bool = Field(default=True, strict=True)
    endpoint: str = Field(default="https://api.jina.ai/v1/embeddings", max_length=2048)
    deployment_id: str = Field(default="jina-clip-v2", min_length=1, max_length=128)
    model: Literal["jina-clip-v2", "Qwen/Qwen3-VL-Embedding-8B"] = "jina-clip-v2"
    model_revision: str = Field(default="v1", min_length=1, max_length=128)
    dimensions: int = Field(default=512, strict=True)
    batch_size: int = Field(default=16, ge=1, le=32, strict=True)
    index_concurrency: int = Field(default=2, ge=1, le=4, strict=True)
    request_timeout: int = Field(default=60, ge=1, le=120, strict=True)
    daily_request_budget: int = Field(default=200, ge=1, le=100000, strict=True)
    daily_token_budget: int = Field(default=200000, ge=1, le=1000000000, strict=True)
    request_token_reservation: int = Field(default=32768, ge=1, le=1000000, strict=True)
    media_allowed_origins: list[str] = Field(default_factory=list, max_length=32)
    capture_concurrency: int = Field(default=2, ge=1, le=4, strict=True)
    capture_timeout: int = Field(default=15, ge=1, le=30, strict=True)
    capture_daily_bytes: int = Field(default=67108864, ge=1, le=2**53 - 1, strict=True)
    capture_storage_bytes: int = Field(default=536870912, ge=1, le=2**53 - 1, strict=True)
    media_max_bytes: int = Field(default=4194304, ge=1, le=4194304, strict=True)
    capture_max_attempts: int = Field(default=3, ge=1, le=10, strict=True)
    query_max_parts: int = Field(default=8, ge=1, le=32, strict=True)
    query_max_images: int = Field(default=4, ge=0, le=8, strict=True)
    query_limit: int = Field(default=8, ge=1, le=32, strict=True)
    query_timeout: int = Field(default=10, ge=1, le=60, strict=True)
    max_result_tokens: int = Field(default=4000, ge=256, le=16000, strict=True)
    max_result_images: int = Field(default=4, ge=0, le=8, strict=True)
    max_result_bytes: int = Field(default=8388608, ge=1, le=33554432, strict=True)

    @staticmethod
    def _url(value):
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
            return str(httpx.URL(value))
        except (ValueError, httpx.InvalidURL):
            raise ValueError(
                "Use a fixed HTTP URL without credentials, query or fragment"
            ) from None

    @field_validator("endpoint")
    @classmethod
    def endpoint_url(cls, value):
        value = cls._url(value)
        if not urlsplit(value).path.endswith("/embeddings"):
            raise ValueError("Provide the complete /embeddings endpoint")
        return value

    @field_validator("media_allowed_origins")
    @classmethod
    def origins(cls, values):
        result = []
        for value in values:
            if len(value) > 2048:
                raise ValueError("Media origin exceeds its size limit")
            value = cls._url(value)
            if urlsplit(value).path not in {"", "/"}:
                raise ValueError("Media permissions require exact origins without paths")
            result.append(value.rstrip("/"))
        return sorted(set(result))

    @field_validator("deployment_id", "model_revision")
    @classmethod
    def identity(cls, value):
        if not value.strip() or any(ord(c) < 33 or ord(c) == 127 for c in value):
            raise ValueError("Deployment and model revisions need explicit stable identifiers")
        return value

    @field_validator("dimensions")
    @classmethod
    def dimension(cls, value):
        if value not in {64, 128, 256, 512, 768, 1024, 2048, 4096}:
            raise ValueError("Choose a dimension supported by the configured embedding model")
        return value

    @field_validator("dimensions")
    @classmethod
    def model_dimension(cls, value, info):
        model = info.data.get("model", "jina-clip-v2")
        if model == "jina-clip-v2" and value not in {512, 1024}:
            raise ValueError("Jina CLIP v2 supports 512 or 1024 dimensions here")
        return value


DEFAULT_MEMORY_SETTINGS = MemorySettings().model_dump()

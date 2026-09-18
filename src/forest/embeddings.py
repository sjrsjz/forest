"""Native multimodal embedding transport with immutable, attributed facets.

The host supplies authorized MediaSnapshots and a final dispatch permit. This
module performs no media fetches, model discovery, retries, database writes or
secret persistence. A mixed batch returns one vector per input, never a fused
multimodal vector. Protocol reference: https://api.jina.ai/openapi.json
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import struct
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field, replace
from typing import Literal
from urllib.parse import urlsplit

import httpx

from forest.errors import ForestError
from forest.models import MediaSnapshot

JINA_MODEL = "jina-clip-v2"
SILICONFLOW_VL_MODEL = "Qwen/Qwen3-VL-Embedding-8B"
PROTOCOL_REVISIONS = {
    JINA_MODEL: "jina-clip-v2-http-2026-09-v1",
    SILICONFLOW_VL_MODEL: "siliconflow-qwen3-vl-embedding-http-2026-09-v1",
}
# Public compatibility name used by existing Jina records and integrations.
PROTOCOL_REVISION = PROTOCOL_REVISIONS[JINA_MODEL]
MAX_BATCH_ITEMS = 32
MAX_TEXT_BYTES = 8192
MAX_IMAGE_BYTES = 4 * 1024 * 1024
MAX_BATCH_IMAGE_BYTES = 8 * 1024 * 1024
MAX_REQUEST_BYTES = 12 * 1024 * 1024
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_MANIFEST_BYTES = 256 * 1024
SOURCE_FIELDS = frozenset(
    {
        "bot_id",
        "binding_id",
        "conversation_id",
        "group_source_id",
        "trunk_id",
        "platform",
        "channel_kind",
        "channel_id",
        "thread_id",
        "unit_id",
        "source_unit_id",
        "event_id",
        "source_event_id",
        "principal_ref",
        "author_ref",
        "part_id",
        "media_ref",
        "media_revision",
        "payload_id",
        "payload_revision",
        "payload_digest",
        "source_id",
        "source_kind",
        "realm_id",
        "identity_schema",
        "source_revision",
        "source_digest",
        "archive_id",
        "logical_step_id",
        "context_revision",
        "context_view_revision",
        "input_revision",
        "selected_input_end_seq",
        "ingress_seq",
        "visibility",
        "kind",
        "origin",
        "job_id",
        "occurrence_id",
    }
)


def _error(code, message, status=422):
    return ForestError(code, message, status)


def _identifier(value):
    try:
        valid = (
            isinstance(value, str)
            and bool(value)
            and len(value.encode("utf-8")) <= 512
            and all(ord(char) >= 32 and ord(char) != 127 for char in value)
        )
    except UnicodeError:
        valid = False
    if not valid:
        raise _error("EMBEDDING_ID_INVALID", "Embedding identity must be bounded text")
    return value


def _hash(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _encode(value, limit=MAX_MANIFEST_BYTES):
    try:
        data = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise _error("EMBEDDING_JSON_INVALID", "Embedding data must be finite JSON") from None
    if len(data) > limit:
        raise _error("EMBEDDING_TOO_LARGE", "Embedding data exceeds its byte limit")
    return data


def _decode(value, limit=MAX_MANIFEST_BYTES):
    def pairs(items):
        result = {}
        for key, item in items:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = item
        return result

    def constant(value):
        raise ValueError("nonfinite")

    try:
        if isinstance(value, str):
            value = value.encode("utf-8")
        if not isinstance(value, bytes) or len(value) > limit:
            raise ValueError("size")
        return json.loads(value.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise _error(
            "EMBEDDING_JSON_INVALID", "Embedding response or manifest is invalid"
        ) from None


def _source(value):
    source = _decode(value, limit=4096)
    if not isinstance(source, dict) or set(source) - SOURCE_FIELDS:
        raise _error("EMBEDDING_SOURCE_INVALID", "Source contains unsupported attribution fields")
    for item in source.values():
        if item is None:
            continue
        if type(item) is int and 0 <= item < 2**63:
            continue
        if isinstance(item, str):
            try:
                if len(item.encode("utf-8")) <= 512 and all(ord(c) >= 32 for c in item):
                    continue
            except UnicodeError:
                pass
        raise _error("EMBEDDING_SOURCE_INVALID", "Source metadata must contain bounded identifiers")
    return source


@dataclass(frozen=True)
class EmbeddingProfile:
    """Public immutable space identity, without endpoints or credentials.

    The model, declared revision, dimensions and deployment identity belong to
    one Bot for its lifetime. The endpoint and credentials may rotate while the
    operator preserves that embedding space. model_revision is an operator
    declaration; a provider response does not prove a remote weights hash.
    """

    deployment_id: str
    model_revision: str
    model: str = JINA_MODEL
    dimensions: int = 512

    def __post_init__(self):
        _identifier(self.deployment_id)
        _identifier(self.model_revision)
        dimensions = (
            set(range(1, 1025))
            if self.model == JINA_MODEL
            else {64, 128, 256, 512, 768, 1024, 2048, 4096}
            if self.model == SILICONFLOW_VL_MODEL
            else set()
        )
        if type(self.dimensions) is not int or self.dimensions not in dimensions:
            raise _error("EMBEDDING_PROFILE_INVALID", "Unsupported embedding model or dimensions")

    def as_dict(self):
        return {
            "deployment_id": self.deployment_id,
            "model_revision": self.model_revision,
            "model": self.model,
            "dimensions": self.dimensions,
            "protocol_revision": PROTOCOL_REVISIONS[self.model],
            "document_task": None,
            "query_task": "retrieval.query" if self.model == JINA_MODEL else None,
            "normalized": True,
            "truncate": False if self.model == JINA_MODEL else "right",
            "image_input": "authorized_capture_bytes_v1",
            "postprocessing": "l2_float32_v1",
            "mixed_strategy": "independent_facets_v1",
        }

    @property
    def space_id(self):
        return _hash(_encode(self.as_dict()))


@dataclass(frozen=True)
class EmbeddingInput:
    """Exactly one semantic facet; image bytes retain host capture provenance."""

    facet_id: str
    text: str | None = field(default=None, repr=False)
    image: MediaSnapshot | None = field(default=None, repr=False)
    source_json: str = field(default="{}", repr=False)

    def __post_init__(self):
        _identifier(self.facet_id)
        source = _source(self.source_json)
        if (self.text is None) == (self.image is None):
            raise _error("EMBEDDING_INPUT_INVALID", "A facet contains exactly one text or image")
        if self.text is not None:
            try:
                valid = (
                    isinstance(self.text, str)
                    and bool(self.text.strip())
                    and len(self.text.encode("utf-8")) <= MAX_TEXT_BYTES
                )
            except UnicodeError:
                valid = False
            if not valid:
                raise _error("EMBEDDING_INPUT_INVALID", "Text facet must be nonempty and bounded")
        else:
            if (
                not isinstance(self.image, MediaSnapshot)
                or len(self.image.content) > MAX_IMAGE_BYTES
            ):
                raise _error(
                    "EMBEDDING_IMAGE_INVALID", "Image facet requires a bounded managed capture"
                )
            for key in ("bot_id", "media_ref", "media_revision", "source_unit_id", "part_id"):
                if key in source and source[key] != getattr(self.image, key):
                    raise _error(
                        "EMBEDDING_SOURCE_INVALID", "Image capture and attribution disagree"
                    )
        object.__setattr__(self, "source_json", _encode(source, limit=4096).decode("utf-8"))

    @property
    def source(self):
        return _decode(self.source_json, limit=4096)


def _materialize(inputs):
    if not isinstance(inputs, (list, tuple)) or not 1 <= len(inputs) <= MAX_BATCH_ITEMS:
        raise _error("EMBEDDING_BATCH_INVALID", "Embedding batch must contain one to 32 facets")
    encoded, facets, identifiers, image_bytes = [], [], set(), 0
    for item in inputs:
        if not isinstance(item, EmbeddingInput) or item.facet_id in identifiers:
            raise _error("EMBEDDING_BATCH_INVALID", "Embedding facets must have unique identities")
        identifiers.add(item.facet_id)
        if item.text is not None:
            data = item.text.encode("utf-8")
            encoded.append({"text": item.text})
            facet = {"modality": "text", "content_digest": _hash(data), "byte_length": len(data)}
        else:
            capture = item.image
            data = capture.content
            image_bytes += len(data)
            encoded.append(
                {
                    "image": base64.b64encode(data).decode("ascii"),
                    "media_type": capture.mime,
                }
            )
            facet = {
                "modality": "image",
                "content_digest": _hash(data),
                "byte_length": len(data),
                "capture": capture.manifest(),
            }
        facets.append({"facet_id": item.facet_id, "source": item.source, **facet})
    if image_bytes > MAX_BATCH_IMAGE_BYTES:
        raise _error("EMBEDDING_BATCH_INVALID", "Batch contains too many image bytes")
    return encoded, facets


@dataclass(frozen=True)
class PreparedEmbeddingRequest:
    request_id: str
    transport_attempt_id: str
    body: bytes = field(repr=False)
    manifest_json: str = field(repr=False)
    payload_digest: str
    space_id: str

    @property
    def manifest(self):
        return _decode(self.manifest_json)

    @property
    def input_count(self):
        return len(self.manifest["facets"])

    def transport_record(self):
        return {
            "request_id": self.request_id,
            "transport_attempt_id": self.transport_attempt_id,
            "payload_digest": self.payload_digest,
            "space_id": self.space_id,
            "protocol_revision": self.manifest["profile"]["protocol_revision"],
        }

    def replay(self, transport_attempt_id):
        """Owner may use this only after establishing that retry is authorized."""
        self.verify()
        _identifier(transport_attempt_id)
        if transport_attempt_id == self.transport_attempt_id:
            raise _error("EMBEDDING_ATTEMPT_INVALID", "A retry requires a new transport attempt")
        return replace(self, transport_attempt_id=transport_attempt_id)

    def verify(self):
        _identifier(self.request_id)
        _identifier(self.transport_attempt_id)
        manifest, body = self.manifest, _decode(self.body, limit=MAX_REQUEST_BYTES)
        try:
            config = manifest["profile"]
            profile = EmbeddingProfile(
                **{k: config[k] for k in ("deployment_id", "model_revision", "model", "dimensions")}
            )
            inputs = []
            for facet, item in zip(manifest["facets"], body["input"], strict=True):
                source = _encode(facet["source"], limit=4096).decode("utf-8")
                if facet["modality"] == "text" and set(item) == {"text"}:
                    inputs.append(
                        EmbeddingInput(facet["facet_id"], text=item["text"], source_json=source)
                    )
                elif facet["modality"] == "image" and set(item) in (
                    {"image"},
                    {"image", "media_type"},
                ):
                    encoded_image = item["image"]
                    if encoded_image.startswith("data:"):
                        header, encoded_image = encoded_image.split(",", 1)
                        if header != "data:" + facet["capture"]["mime"] + ";base64":
                            raise ValueError("media type")
                    content = base64.b64decode(encoded_image, validate=True)
                    capture = MediaSnapshot(**facet["capture"], content=content)
                    inputs.append(
                        EmbeddingInput(facet["facet_id"], image=capture, source_json=source)
                    )
                else:
                    raise ValueError("modality")
            expected = _prepare(
                profile, inputs, self.request_id, self.transport_attempt_id, manifest["purpose"]
            )
            if (
                self.payload_digest != expected.payload_digest
                or self.space_id != expected.space_id
                or self.body != expected.body
                or manifest != expected.manifest
            ):
                raise ValueError("fence")
        except (KeyError, TypeError, ValueError, UnicodeError, ForestError):
            raise _error(
                "EMBEDDING_REQUEST_TAMPERED", "Prepared request lost its fixed bytes or provenance"
            ) from None


def _prepare(profile, inputs, request_id, transport_attempt_id, purpose):
    if not isinstance(profile, EmbeddingProfile) or purpose not in {"document", "query"}:
        raise _error("EMBEDDING_PROFILE_INVALID", "Embedding profile or purpose is invalid")
    _identifier(request_id)
    _identifier(transport_attempt_id)
    encoded, facets = _materialize(inputs)
    if profile.model == JINA_MODEL:
        body = {
            "model": profile.model,
            "dimensions": profile.dimensions,
            "embedding_type": "float",
            "normalized": True,
            "truncate": False,
            "input": [
                ({"text": item["text"]} if "text" in item else {"image": item["image"]})
                for item in encoded
            ],
        }
        if purpose == "query":
            body["task"] = "retrieval.query"
    else:
        body = {
            "model": profile.model,
            "dimensions": profile.dimensions,
            "encoding_format": "float",
            "truncate": "right",
            "input": [
                (
                    {"text": item["text"]}
                    if "text" in item
                    else {
                        "image": "data:"
                        + item["media_type"]
                        + ";base64,"
                        + item["image"]
                    }
                )
                for item in encoded
            ],
        }
    wire = _encode(body, limit=MAX_REQUEST_BYTES)
    manifest = {
        "request_id": request_id,
        "profile": profile.as_dict(),
        "space_id": profile.space_id,
        "purpose": purpose,
        "facets": facets,
        "input_digest": _hash(_encode(facets)),
        "payload_digest": _hash(wire),
    }
    return PreparedEmbeddingRequest(
        request_id,
        transport_attempt_id,
        wire,
        _encode(manifest).decode("utf-8"),
        _hash(wire),
        profile.space_id,
    )


def prepare_embedding_request(
    profile: EmbeddingProfile,
    inputs: Sequence[EmbeddingInput],
    *,
    request_id: str,
    transport_attempt_id: str,
    purpose: Literal["document", "query"] = "document",
) -> PreparedEmbeddingRequest:
    """Freeze one bounded batch without network calls or hidden image conversions."""
    # Reject unbounded generators rather than consuming them into memory.
    return _prepare(profile, inputs, request_id, transport_attempt_id, purpose)


@dataclass(frozen=True)
class EmbeddingVector:
    facet_id: str
    index: int
    values: tuple[float, ...] = field(repr=False)


@dataclass(frozen=True)
class EmbeddingResult:
    status: Literal["RESPONDED", "REJECTED", "NOT_SENT", "UNKNOWN"]
    request_id: str
    transport_attempt_id: str
    payload_digest: str
    space_id: str
    vectors: tuple[EmbeddingVector, ...] = field(default=(), repr=False)
    usage_json: str | None = field(default=None, repr=False)
    error_code: str | None = None
    http_status: int | None = None

    @property
    def usage(self):
        return _decode(self.usage_json) if self.usage_json is not None else None

    def as_dict(self):
        return {
            "status": self.status,
            "request_id": self.request_id,
            "transport_attempt_id": self.transport_attempt_id,
            "payload_digest": self.payload_digest,
            "space_id": self.space_id,
            "vectors": [
                {"facet_id": v.facet_id, "index": v.index, "values": list(v.values)}
                for v in self.vectors
            ],
            "usage": self.usage,
            "error_code": self.error_code,
            "http_status": self.http_status,
        }


class EmbeddingCancelled(asyncio.CancelledError):
    """Cancellation propagates, with a safe observation for the durable owner."""

    def __init__(self, result):
        super().__init__("Embedding transport cancelled")
        self.result = result


def _failure(prepared, status, code, http_status=None):
    return EmbeddingResult(
        status,
        prepared.request_id,
        prepared.transport_attempt_id,
        prepared.payload_digest,
        prepared.space_id,
        error_code=code,
        http_status=http_status,
    )


def _completion(prepared, payload):
    manifest = prepared.manifest
    if (
        not isinstance(payload, dict)
        or payload.get("model") != manifest["profile"]["model"]
        or payload.get("object", "list") != "list"
        or not isinstance(payload.get("data"), list)
        or len(payload["data"]) != len(manifest["facets"])
    ):
        raise _error("EMBEDDING_RESPONSE_INVALID", "Embedding response differs from this request")
    usage = payload.get("usage")
    if not isinstance(usage, dict) or type(usage.get("total_tokens")) is not int:
        raise _error("EMBEDDING_USAGE_INVALID", "Embedding usage must contain nonnegative counters")
    if set(usage) - {
        "total_tokens",
        "prompt_tokens",
        "image_tokens",
        "audio_tokens",
        "video_tokens",
        "completion_tokens",
    }:
        raise _error("EMBEDDING_USAGE_INVALID", "Embedding usage contains unsupported fields")
    usage = {key: value for key, value in usage.items() if value is not None}
    if any(type(value) is not int or not 0 <= value < 2**63 for value in usage.values()):
        raise _error("EMBEDDING_USAGE_INVALID", "Embedding usage must contain nonnegative counters")
    # Jina's explicit additive modality invariant is scoped to v5-omni. CLIP
    # counters can overlap; preserve them without deriving a second aggregate.
    if any(value > usage["total_tokens"] for value in usage.values()):
        raise _error("EMBEDDING_USAGE_INVALID", "Usage counter exceeds the reported aggregate")
    vectors = {}
    for item in payload["data"]:
        if not isinstance(item, dict) or item.get("object", "embedding") != "embedding":
            raise _error("EMBEDDING_RESPONSE_INVALID", "Embedding item is invalid")
        index, vector = item.get("index"), item.get("embedding")
        if type(index) is not int or not 0 <= index < len(manifest["facets"]) or index in vectors:
            raise _error(
                "EMBEDDING_INDEX_INVALID", "Embedding indexes must cover the input exactly once"
            )
        if not isinstance(vector, list) or len(vector) != manifest["profile"]["dimensions"]:
            raise _error(
                "EMBEDDING_DIMENSION_INVALID", "Embedding dimensions differ from the frozen space"
            )
        try:
            finite = all(type(v) in {float, int} and math.isfinite(v) for v in vector)
        except OverflowError:
            finite = False
        if not finite:
            raise _error("EMBEDDING_VECTOR_INVALID", "Embedding components must be finite numbers")
        norm = math.hypot(*vector)
        if not math.isfinite(norm) or norm <= 0:
            raise _error("EMBEDDING_VECTOR_INVALID", "Provider returned a zero or invalid vector")
        if manifest["profile"]["model"] == JINA_MODEL and abs(norm - 1.0) > 0.01:
            raise _error("EMBEDDING_VECTOR_INVALID", "Provider did not return a normalized vector")
        values = tuple(struct.unpack("<f", struct.pack("<f", v / norm))[0] for v in vector)
        vectors[index] = EmbeddingVector(manifest["facets"][index]["facet_id"], index, values)
    return EmbeddingResult(
        "RESPONDED",
        prepared.request_id,
        prepared.transport_attempt_id,
        prepared.payload_digest,
        prepared.space_id,
        tuple(vectors[i] for i in range(len(vectors))),
        _encode(usage).decode("utf-8"),
        http_status=200,
    )


PermitCheck = Callable[[PreparedEmbeddingRequest], Awaitable[object]]


class EmbeddingClient:
    """Fixed endpoint/profile, bounded pools, no redirects, proxies or retries."""

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        profile: EmbeddingProfile,
        *,
        timeout_seconds: float = 60,
        max_connections: int = 4,
    ):
        try:
            if (
                not isinstance(endpoint, str)
                or len(endpoint) > 2048
                or any(ord(c) <= 32 or ord(c) == 127 for c in endpoint)
                or "\\" in endpoint
            ):
                raise ValueError("URL")
            parsed = urlsplit(endpoint)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
                or not parsed.path.endswith("/embeddings")
                or parsed.port == 0
            ):
                raise ValueError("URL")
            self._endpoint = str(httpx.URL(endpoint))
        except (ValueError, httpx.InvalidURL):
            raise _error("EMBEDDING_CONFIG_INVALID", "Invalid fixed embeddings endpoint") from None
        if (
            not isinstance(profile, EmbeddingProfile)
            or not isinstance(api_key, str)
            or len(api_key) > 4096
            or any(not 33 <= ord(c) <= 126 for c in api_key)
            or type(max_connections) is not int
            or not 1 <= max_connections <= 32
        ):
            raise _error(
                "EMBEDDING_CONFIG_INVALID", "Invalid embedding profile or transport configuration"
            )
        self._duration(timeout_seconds)
        self.profile, self.timeout_seconds = profile, timeout_seconds
        self._slots = asyncio.Semaphore(max_connections)
        self._tasks, self._closing_tasks = set(), set()
        self._closed, self._close_task = False, None
        limits = httpx.Limits(
            max_connections=max_connections, max_keepalive_connections=max_connections
        )
        self._client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=0, limits=limits, trust_env=False),
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(timeout_seconds),
            headers={
                **({"Authorization": "Bearer " + api_key} if api_key else {}),
                "Accept-Encoding": "identity",
            },
        )

    @staticmethod
    def _duration(value):
        if type(value) not in {int, float} or not math.isfinite(value) or not 0 < value <= 3600:
            raise _error("EMBEDDING_CONFIG_INVALID", "Embedding timeout must be finite and bounded")

    async def dispatch(
        self,
        prepared: PreparedEmbeddingRequest,
        *,
        permitcheck: PermitCheck,
        timeout: float | None = None,
    ) -> EmbeddingResult:
        prepared.verify()
        if prepared.manifest["profile"] != self.profile.as_dict():
            raise _error(
                "EMBEDDING_ROUTE_MISMATCH", "Request belongs to a different embedding space"
            )
        duration = self.timeout_seconds if timeout is None else timeout
        self._duration(duration)
        if self._closed:
            return _failure(prepared, "NOT_SENT", "EMBEDDING_CLIENT_CLOSED")
        # Own only the transport coroutine. The caller may catch cancellation
        # and perform unrelated work or close this client in its own finally.
        task = asyncio.create_task(
            self._dispatch(prepared, permitcheck=permitcheck, duration=duration),
            name="embedding-transport",
        )
        self._tasks.add(task)
        task.add_done_callback(self._transport_done)
        try:
            return await task
        except EmbeddingCancelled:
            raise
        except asyncio.CancelledError:
            # The owned coroutine may be cancelled before its first turn.
            raise EmbeddingCancelled(
                _failure(prepared, "NOT_SENT", "EMBEDDING_CANCELLED")
            ) from None

    def _transport_done(self, task):
        self._tasks.discard(task)
        self._closing_tasks.discard(task)
        if not task.cancelled():
            task.exception()

    async def _dispatch(self, prepared, *, permitcheck, duration):
        started = False
        try:
            if self._closed:
                return _failure(prepared, "NOT_SENT", "EMBEDDING_CLIENT_CLOSED")
            async with asyncio.timeout(duration) as deadline:
                async with self._slots:
                    if self._closed:
                        return _failure(prepared, "NOT_SENT", "EMBEDDING_CLIENT_CLOSED")
                    # The callback owns policy/lease/source validation. Its
                    # exceptions propagate; they are not Provider error strings.
                    await permitcheck(prepared)
                    if self._closed:
                        return _failure(prepared, "NOT_SENT", "EMBEDDING_CLIENT_CLOSED")
                    # A host callback must not consume cancellation/expiry and
                    # then allow a new effect across this final network fence.
                    if asyncio.current_task().cancelling() or deadline.expired():
                        raise asyncio.CancelledError
                    started = True
                    async with self._client.stream(
                        "POST",
                        self._endpoint,
                        content=prepared.body,
                        headers={"Content-Type": "application/json", "Accept": "application/json"},
                        timeout=duration,
                    ) as response:
                        status = response.status_code
                        if status != 200:
                            disposition = "REJECTED" if 300 <= status < 500 else "UNKNOWN"
                            return _failure(
                                prepared,
                                disposition,
                                "EMBEDDING_HTTP_REJECTED"
                                if disposition == "REJECTED"
                                else "EMBEDDING_HTTP_UNCERTAIN",
                                status,
                            )
                        if (
                            response.headers.get("content-type", "")
                            .split(";", 1)[0]
                            .strip()
                            .lower()
                            != "application/json"
                            or response.headers.get("content-encoding", "identity").lower()
                            != "identity"
                        ):
                            return _failure(
                                prepared, "UNKNOWN", "EMBEDDING_RESPONSE_ENCODING", status
                            )
                        content = bytearray()
                        async for chunk in response.aiter_raw():
                            if len(content) + len(chunk) > MAX_RESPONSE_BYTES:
                                return _failure(
                                    prepared, "UNKNOWN", "EMBEDDING_RESPONSE_TOO_LARGE", status
                                )
                            content.extend(chunk)
                        try:
                            return _completion(
                                prepared, _decode(bytes(content), limit=MAX_RESPONSE_BYTES)
                            )
                        except ForestError as exc:
                            return _failure(prepared, "UNKNOWN", exc.code, status)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
            return _failure(prepared, "NOT_SENT", "EMBEDDING_CONNECTION_UNAVAILABLE")
        except TimeoutError:
            return _failure(prepared, "UNKNOWN" if started else "NOT_SENT", "EMBEDDING_TIMEOUT")
        except asyncio.CancelledError:
            raise EmbeddingCancelled(
                _failure(prepared, "UNKNOWN" if started else "NOT_SENT", "EMBEDDING_CANCELLED")
            ) from None
        except (httpx.HTTPError, OSError):
            return _failure(
                prepared, "UNKNOWN" if started else "NOT_SENT", "EMBEDDING_TRANSPORT_UNCERTAIN"
            )

    async def aclose(self):
        self._closed = True
        if asyncio.current_task() in self._closing_tasks:
            return
        if self._close_task is None or (
            self._close_task.done()
            and (self._close_task.cancelled() or self._close_task.exception() is not None)
        ):
            self._close_task = asyncio.create_task(self._close(), name="close:embedding-client")
        await asyncio.shield(self._close_task)

    async def _close(self):
        tasks = [task for task in self._tasks if task is not asyncio.current_task()]
        self._closing_tasks.update(tasks)
        pending = set()
        pool_failed = False
        try:
            for task in tasks:
                task.cancel()
            if tasks:
                # A host permit callback may suppress cancellation. Do not let
                # it hold shutdown forever; retain ownership and report it.
                _, pending = await asyncio.wait(tasks, timeout=1)
        finally:
            try:
                async with asyncio.timeout(1):
                    await self._client.aclose()
            except (TimeoutError, OSError, httpx.HTTPError):
                pool_failed = True
            self._closing_tasks.difference_update(task for task in tasks if task.done())
        if any(not task.done() for task in pending) or pool_failed:
            raise _error("EMBEDDING_CLEANUP_FAILED", "Embedding transport resources remain owned")

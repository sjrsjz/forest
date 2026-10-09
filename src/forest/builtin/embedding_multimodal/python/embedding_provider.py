import base64
import hashlib
import io
import json

from PIL import Image, ImageOps, UnidentifiedImageError

from forest.embedding_contract import IMAGE_INPUT_POLICY
from forest.errors import ForestError
from forest.sdk import Plugin, service

MAX_EMBEDDING_IMAGE_BYTES = 4 * 1024 * 1024


def _fit_image(content, width, height):
    """Freeze a bounded rendition while retaining the original Payload as parent."""

    try:
        with Image.open(io.BytesIO(content)) as original:
            if original.size != (width, height) or width * height > 40_000_000:
                raise ForestError(
                    "EMBEDDING_FACET_INVALID", "Archived image dimensions are invalid", 422
                )
            image = ImageOps.exif_transpose(original)
            if image.mode in {"RGBA", "LA", "P"}:
                rgba = image.convert("RGBA")
                background = Image.new("RGB", rgba.size, "white")
                background.paste(rgba, mask=rgba.getchannel("A"))
                image = background
            else:
                image = image.convert("RGB")
            for side in (2048, 1536, 1024, 768):
                candidate = image.copy()
                candidate.thumbnail((side, side), Image.Resampling.LANCZOS)
                output = io.BytesIO()
                candidate.save(output, format="JPEG", quality=82, subsampling=0)
                rendition = output.getvalue()
                if len(rendition) <= MAX_EMBEDDING_IMAGE_BYTES:
                    return rendition, candidate.size
    except (UnidentifiedImageError, OSError, Image.DecompressionBombError) as exc:
        raise ForestError(
            "EMBEDDING_FACET_INVALID", "Archived image cannot be decoded", 422
        ) from exc
    raise ForestError(
        "EMBEDDING_FACET_INVALID", "Archived image cannot fit its embedding limit", 422
    )


def _plain(value):
    if hasattr(value, "items"):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _digest(value):
    return _bytes_digest(_canonical(value))


def _canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _bytes_digest(value):
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _profile_snapshot(profile):
    model = profile["model"]
    if model == "jina-clip-v2":
        protocol_revision = "jina-clip-v2-http-2026-09-v1"
        query_task = "retrieval.query"
        truncate = False
    elif model == "Qwen/Qwen3-VL-Embedding-8B":
        protocol_revision = "siliconflow-qwen3-vl-embedding-http-2026-09-v1"
        query_task = None
        truncate = "right"
    else:
        raise ForestError(
            "EMBEDDING_CONFIGURATION_INVALID", "Embedding model is unsupported", 422
        )
    return {
        **profile,
        "protocol_revision": protocol_revision,
        "document_task": None,
        "query_task": query_task,
        "normalized": True,
        "truncate": truncate,
        "image_input": IMAGE_INPUT_POLICY,
        "postprocessing": "l2_float32_v1",
        "mixed_strategy": "independent_facets_v1",
    }


def _max_batch_items(model):
    # SiliconFlow resets result indexes after each group of eight. The wire
    # contract cannot attribute a larger response to its frozen facet list.
    return 8 if model == "Qwen/Qwen3-VL-Embedding-8B" else 32


def _facet_source(facet, bot_id):
    if facet.get("input_kind") == "ADMIN_TRIAL":
        source = {
            "bot_id": bot_id,
            "input_kind": "ADMIN_TRIAL",
            "input_ref": facet["input_ref"],
            "input_revision": facet["input_revision"],
            "query_id": facet["query_id"],
            "part_id": facet["part_id"],
            "payload_id": facet["input_payload"]["payload_id"],
            "payload_digest": facet["input_payload"]["digest"],
        }
        if facet["input_ref"] != facet["query_id"]:
            raise ForestError(
                "EMBEDDING_FACET_INVALID", "Trial query identity differs", 422
            )
        if facet["modality"] == "IMAGE":
            source["media_ref"] = facet["input_payload"]["payload_id"]
        return source
    source = {
        "bot_id": bot_id,
        "source_unit_id": facet["source_unit_id"],
        "source_revision": facet["source_revision"],
        "part_id": facet["part_id"],
        "trunk_id": facet["trunk_id"],
        "conversation_id": facet["conversation_ref"],
        "author_ref": facet["author_ref"],
        "payload_id": facet["input_payload"]["payload_id"],
        "payload_digest": facet["input_payload"]["digest"],
    }
    if facet["modality"] == "IMAGE":
        source["media_ref"] = facet["input_payload"]["payload_id"]
        source["media_revision"] = facet["source_revision"]
    return source


async def _embedding_input(facet, payloads, bot_id):
    content = await payloads.read_bytes(facet["input_payload"])
    if facet["modality"] == "TEXT":
        try:
            original = content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ForestError(
                "EMBEDDING_FACET_INVALID", "Archived text is not UTF-8", 422
            ) from exc
        start, end = facet["selector_start_char"], facet["selector_end_char"]
        if (
            start is None or end is None or start < 0 or end <= start
            or end > len(original) or facet["width"] is not None
            or facet["height"] is not None
        ):
            raise ForestError(
                "EMBEDDING_FACET_INVALID", "Archived text span is invalid", 422
            )
        text = original[start:end]
        selected = text.encode("utf-8")
        if not text.strip() or len(selected) > 8192:
            raise ForestError(
                "EMBEDDING_FACET_INVALID", "Archived text facet is not embeddable", 422
            )
        wire = {"text": text}
        capture = None
    else:
        if (
            facet["selector_start_char"] is not None
            or facet["selector_end_char"] is not None
            or facet["width"] is None or facet["height"] is None
        ):
            raise ForestError(
                "EMBEDDING_FACET_INVALID", "Archived image selector is invalid", 422
            )
        if (
            facet["media_type"] not in {"image/png", "image/jpeg", "image/webp", "image/gif"}
            or not 0 < len(content) <= 32 * 1024 * 1024
        ):
            raise ForestError(
                "EMBEDDING_FACET_INVALID", "Archived image facet is not embeddable", 422
            )
        if (
            len(content) != facet["input_byte_length"]
            or _bytes_digest(content) != facet["facet_content_digest"]
        ):
            raise ForestError(
                "EMBEDDING_FACET_INVALID", "Archived facet bytes differ from their identity", 409
            )
        selected = content
        capture_mime = facet["media_type"]
        capture_width, capture_height = facet["width"], facet["height"]
        transform_ref = None
        if len(content) > MAX_EMBEDDING_IMAGE_BYTES:
            selected, (capture_width, capture_height) = _fit_image(
                content, capture_width, capture_height
            )
            capture_mime = "image/jpeg"
            transform_ref = "image-transform:" + _digest([
                IMAGE_INPUT_POLICY, facet["facet_content_digest"],
                _bytes_digest(selected), capture_width, capture_height,
            ])
        capture = {
            "bot_id": bot_id,
            "media_ref": facet["input_payload"]["payload_id"],
            "part_id": facet["part_id"],
            "mime": capture_mime,
            "blob_digest": _bytes_digest(selected),
            "width": capture_width,
            "height": capture_height,
            "transform_ref": transform_ref,
        }
        if facet.get("input_kind") == "ADMIN_TRIAL":
            capture.update({
                "input_kind": "ADMIN_TRIAL",
                "query_id": facet["query_id"],
                "input_revision": facet["input_revision"],
            })
        else:
            capture.update({
                "media_revision": facet["source_revision"],
                "source_unit_id": facet["source_unit_id"],
            })
        wire = {"image": base64.b64encode(selected).decode("ascii")}
    if (
        facet["modality"] == "TEXT" and (
            len(selected) != facet["input_byte_length"]
            or _bytes_digest(selected) != facet["facet_content_digest"]
        )
    ):
        raise ForestError(
            "EMBEDDING_FACET_INVALID", "Archived facet bytes differ from their identity", 409
        )
    manifest = {
        "facet_id": facet["facet_id"],
        "source": _facet_source(facet, bot_id),
        "modality": "image" if capture is not None else "text",
        "content_digest": _bytes_digest(selected),
        "byte_length": len(selected),
    }
    if capture is not None:
        manifest["capture"] = capture
    return wire, manifest, len(selected) if capture is not None else 0


async def _prepare_request(request, profile, space_id, payloads, bot_id):
    facets = request["facets"]
    if (not 1 <= len(facets) <= _max_batch_items(profile["model"])
            or len({f["facet_id"] for f in facets}) != len(facets)):
        raise ForestError(
            "EMBEDDING_BATCH_INVALID", "Embedding batch has invalid facet identities", 422
        )
    inputs, manifest_facets, image_bytes = [], [], 0
    for facet in facets:
        try:
            wire, manifest, image_size = await _embedding_input(facet, payloads, bot_id)
        except ForestError as exc:
            if exc.code != "EMBEDDING_FACET_INVALID":
                raise
            raise ForestError(
                exc.code, exc.message, exc.status,
                details={"facet_id": facet["facet_id"],
                         "facet_content_digest": facet["facet_content_digest"]},
            ) from exc
        inputs.append(wire)
        manifest_facets.append(manifest)
        image_bytes += image_size
    if image_bytes > 8 * 1024 * 1024:
        raise ForestError(
            "EMBEDDING_BATCH_INVALID", "Embedding batch contains too many image bytes", 422
        )
    if profile["model"] == "jina-clip-v2":
        body = {
            "model": profile["model"],
            "dimensions": profile["dimensions"],
            "embedding_type": "float",
            "normalized": True,
            "truncate": False,
            "input": inputs,
        }
        if request["purpose"] == "QUERY":
            body["task"] = "retrieval.query"
    else:
        body = {
            "model": profile["model"],
            "dimensions": profile["dimensions"],
            "encoding_format": "float",
            "truncate": "right",
            "input": [
                item if "text" in item else {
                    "image": "data:" + facet["capture"]["mime"] + ";base64," + item["image"]
                }
                for item, facet in zip(inputs, manifest_facets, strict=True)
            ],
        }
    body_bytes = _canonical(body)
    if len(body_bytes) > 12 * 1024 * 1024:
        raise ForestError(
            "EMBEDDING_BATCH_INVALID", "Embedding request exceeds its byte limit", 422
        )
    manifest = {
        "request_id": request["request_id"],
        "profile": profile,
        "space_id": space_id,
        "purpose": request["purpose"].lower(),
        "facets": manifest_facets,
        "input_digest": _digest(manifest_facets),
        "payload_digest": _bytes_digest(body_bytes),
    }
    manifest_bytes = _canonical(manifest)
    if len(manifest_bytes) > 256 * 1024:
        raise ForestError(
            "EMBEDDING_BATCH_INVALID", "Embedding manifest exceeds its byte limit", 422
        )
    return body_bytes, manifest_bytes


class MultimodalEmbeddingProvider(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        plain = _plain(config)
        if plain["protocol"] != "forest.embedding.multimodal/1":
            raise ForestError(
                "EMBEDDING_CONFIGURATION_INVALID", "Embedding protocol is unsupported", 422
            )
        self.profile = plain["profile"]
        self.snapshot = _profile_snapshot(self.profile)
        self.space_id = _digest(self.snapshot)
        self.profile_ref = "profile:" + _digest(plain)[7:]

    def replacement_facts(self):
        return {"space": {
            "space_id": self.space_id,
            "dimensions": self.profile["dimensions"],
        }}

    @service(
        "embedding.profile.bind",
        semantics="read",
        contract="2.0",
        input_schema="schemas/bind-input.json",
        output_schema="schemas/bind-output.json",
    )
    async def bind(self, payload, ctx):
        return {
            "protocol": "forest.embedding-profile/2",
            "request_id": payload["request_id"],
            "component_id": "embedding.multimodal",
            "generation_id": ctx.identity["generation_id"],
            "profile_ref": self.profile_ref,
            "space_id": self.space_id,
            "profile": self.profile,
            "max_batch_items": _max_batch_items(self.profile["model"]),
        }

    @service(
        "embedding.request.materialize",
        semantics="idempotent",
        contract="1.0",
        input_schema="schemas/materialize-input.json",
        output_schema="schemas/materialize-output.json",
    )
    async def materialize(self, payload, ctx):
        request = _plain(payload)
        if (
            request["component_id"] != "embedding.multimodal"
            or request["generation_id"] != ctx.identity["generation_id"]
            or request["profile_ref"] != self.profile_ref
            or request["space_id"] != self.space_id
        ):
            raise ForestError(
                "EMBEDDING_REQUEST_FENCE_MISMATCH",
                "Materialization belongs to another embedding Generation", 409,
            )
        body_bytes, manifest_bytes = await _prepare_request(
            request, self.snapshot, self.space_id, ctx.payloads,
            ctx.identity["bot_id"],
        )
        parents = [facet["input_payload"] for facet in request["facets"]]
        body = await ctx.payloads.put_bytes(
            body_bytes,
            owner_kind="embedding.request.body",
            owner_id=request["request_id"],
            media_type="application/json",
            parents=parents,
        )
        manifest = await ctx.payloads.put_bytes(
            manifest_bytes,
            owner_kind="embedding.request.manifest",
            owner_id=request["request_id"],
            media_type="application/json",
            parents=[body, *parents],
        )
        return {
            "protocol": "forest.embedding-materialized/1",
            "request_id": request["request_id"],
            "transport_attempt_id": request["transport_attempt_id"],
            "component_id": "embedding.multimodal",
            "generation_id": ctx.identity["generation_id"],
            "profile_ref": self.profile_ref,
            "space_id": self.space_id,
            "prepared": {
                "request_id": request["request_id"],
                "transport_attempt_id": request["transport_attempt_id"],
                "body": body,
                "manifest": manifest,
                "payload_digest": _bytes_digest(body_bytes),
                "space_id": self.space_id,
            },
        }

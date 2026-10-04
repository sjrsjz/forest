"""Generation-owned public image acquisition for the media.capture package."""

from __future__ import annotations

from datetime import datetime

from forest.errors import ForestError
from forest.media_capture_io import PublicImageCaptureClient
from forest.storage.payload_authority import PayloadSource


class MediaImageFetchEffectDriver:
    """Own sockets and Payload writes without owning Media business state."""

    def __init__(
        self,
        configuration,
        _configuration_revision,
        identity,
        declaration,
        *,
        runtime,
        payloads,
        client_factory=PublicImageCaptureClient,
    ) -> None:
        if (
            declaration.get("kind") != "media.public-image"
            or declaration.get("name") != "media.image.fetch"
        ):
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "Media image driver received another effect service",
                500,
            )
        self.configuration = configuration
        self.identity = identity
        self.runtime = runtime
        self.payloads = payloads
        self.client_factory = client_factory
        self.client = None
        self.accepting = False

    async def start(self) -> None:
        self.client = self.client_factory(
            timeout_seconds=self.configuration["timeout_seconds"],
            max_connections=self.configuration["max_connections"],
        )
        self.accepting = True

    async def invoke(self, payload, *, scope, invocation):
        if (
            not self.accepting
            or self.client is None
            or scope.component_id != self.identity.component_id
            or scope.generation_id != self.identity.generation_id
        ):
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Media Generation is not accepting image effects",
                409,
            )
        await self.runtime.verify_generation_member(
            component_id=self.identity.component_id,
            generation_id=self.identity.generation_id,
        )
        image = await self.client.fetch(payload["locator"])
        provenance = payload["provenance"]
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id="media.capture",
            owner_kind="media.asset",
            owner_id=payload["media_ref"],
            media_type=image.media_type,
            content=image.content,
            sources=(
                PayloadSource(
                    provenance["source_component_id"],
                    provenance["source_kind"],
                    provenance["source_id"],
                    provenance["source_revision"],
                    part_id=provenance["part_id"],
                    source_trunk_id=provenance.get("source_trunk_id"),
                    observation_conversation_id=provenance.get("conversation_ref"),
                    observation_binding_id=provenance.get("binding_ref"),
                    author_subject_id=provenance.get("author_subject_ref"),
                    occurred_at=datetime.fromisoformat(provenance["occurred_at"]),
                ),
            ),
            expected_deletion_epoch=epoch,
        )
        return {
            "protocol": "forest.media-image-fetch-result/2",
            "media_ref": payload["media_ref"],
            "payload": {
                "payload_id": saved.payload_id,
                "digest": saved.content_digest,
                "owner_component_id": "media.capture",
                "owner_kind": "media.asset",
                "owner_id": payload["media_ref"],
            },
            "media_type": image.media_type,
            "width": image.width,
            "height": image.height,
            "decoded_pixels": image.decoded_pixels,
            "byte_length": len(image.content),
            "operation_id": invocation.operation_id,
        }

    async def quiesce(self) -> None:
        self.accepting = False

    async def resume(self) -> None:
        if self.client is None:
            await self.start()
        else:
            self.accepting = True

    async def close(self) -> None:
        self.accepting = False
        client, self.client = self.client, None
        if client is not None:
            await client.aclose()


def media_image_fetch_effect_driver_factory(
    *, runtime, payloads, client_factory=PublicImageCaptureClient
):
    def create(configuration, configuration_revision, identity, declaration):
        return MediaImageFetchEffectDriver(
            configuration,
            configuration_revision,
            identity,
            declaration,
            runtime=runtime,
            payloads=payloads,
            client_factory=client_factory,
        )

    return create

"""Public ingress DTOs for the media.capture package participant."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from forest.errors import ForestError
from forest.storage.memory_graph_authority import MemoryPart, MemoryPayloadRef, MemorySourceIdentity


def _bounded(value, field: str, limit: int = 256) -> None:
    if not isinstance(value, str) or not 1 <= len(value.encode()) <= limit:
        raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", f"Invalid {field}", 422)


@dataclass(frozen=True)
class MediaImageCapture:
    part_id: str
    part_index: int
    media_ref: str
    locator: dict

    def validate(self) -> None:
        _bounded(self.part_id, "part_id")
        _bounded(self.media_ref, "media_ref")
        if type(self.part_index) is not int or not 0 <= self.part_index <= 4095:
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid image part index", 422)
        if (
            not isinstance(self.locator, dict)
            or set(self.locator) != {"kind", "uri", "platform_file"}
            or self.locator.get("kind") != "onebot-source"
            or not any(self.locator.get(name) for name in ("uri", "platform_file"))
        ):
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid media locator", 422)
        for name in ("uri", "platform_file"):
            value = self.locator.get(name)
            if value is not None and (not isinstance(value, str) or len(value.encode()) > 8192):
                raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid media locator", 422)

    def canonical(self) -> dict:
        return {
            "part_id": self.part_id,
            "part_index": self.part_index,
            "media_ref": self.media_ref,
            "locator": self.locator,
        }


@dataclass(frozen=True)
class IngressMediaCapture:
    capture_id: str
    identity: MemorySourceIdentity
    source_unit_id: str
    source_revision: int
    unit_kind: str
    author: MemoryPayloadRef
    text_parts: tuple[MemoryPart, ...]
    image_parts: tuple[MediaImageCapture, ...]
    occurred_at: datetime

    def validate(self) -> None:
        _bounded(self.capture_id, "capture_id")
        _bounded(self.source_unit_id, "source_unit_id")
        _bounded(self.unit_kind, "unit_kind", 128)
        self.identity.validate()
        self.author.validate("author")
        if type(self.source_revision) is not int or self.source_revision < 1:
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid source revision", 422)
        if not isinstance(self.occurred_at, datetime) or self.occurred_at.tzinfo is None:
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid occurrence time", 422)
        parts = (*self.text_parts, *self.image_parts)
        if not 1 <= len(parts) <= 4096 or not self.image_parts:
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid capture parts", 422)
        for part in self.text_parts:
            part.validate()
            if part.part_kind != "TEXT":
                raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Text part is not text", 422)
        for part in self.image_parts:
            part.validate()
        identities = [(part.part_id, part.part_index) for part in parts]
        if len({item[0] for item in identities}) != len(parts) or len(
            {item[1] for item in identities}
        ) != len(parts):
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Capture parts repeat", 422)

    def canonical(self) -> dict:
        self.validate()
        return {
            "capture_id": self.capture_id,
            "identity": self.identity.canonical(),
            "source_unit_id": self.source_unit_id,
            "source_revision": self.source_revision,
            "unit_kind": self.unit_kind,
            "author": self.author.canonical(),
            "text_parts": [part.canonical() for part in self.text_parts],
            "image_parts": [part.canonical() for part in self.image_parts],
            "occurred_at": self.occurred_at.isoformat(),
        }

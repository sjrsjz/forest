"""Immutable resource limits shared by Graph planning and Python Worker launch."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

MAX_FRAME_BYTES = 256 * 1024


@dataclass(frozen=True)
class WorkerLimits:
    max_frame_bytes: int = MAX_FRAME_BYTES
    max_inflight: int = 16
    max_queue_items: int = 64
    max_state_inflight: int = 16
    max_owned_tasks: int = 64
    max_owned_resources: int = 64

    @classmethod
    def from_component(cls, component: Mapping[str, Any]) -> WorkerLimits:
        declared = component.get("resources", {})
        defaults = cls()
        return cls(**{
            key: declared.get(key, getattr(defaults, key))
            for key in (
                "max_frame_bytes", "max_inflight", "max_queue_items",
                "max_state_inflight", "max_owned_tasks", "max_owned_resources",
            )
        })

    def as_fact(self) -> dict[str, int]:
        return asdict(self)

    def __post_init__(self):
        if type(self.max_frame_bytes) is not int or not 1024 <= self.max_frame_bytes <= MAX_FRAME_BYTES:
            raise ValueError("Frame limit must be 1024 to 262144 bytes")
        for value in (
            self.max_inflight,
            self.max_state_inflight,
            self.max_owned_tasks,
            self.max_owned_resources,
        ):
            if type(value) is not int or not 1 <= value <= 256:
                raise ValueError("Execution limits must be 1 to 256")
        if type(self.max_queue_items) is not int or not 0 <= self.max_queue_items <= 256:
            raise ValueError("Queue size must be 0 to 256")

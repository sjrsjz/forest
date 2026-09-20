"""Typed service boundary between the durable Job scheduler and executors."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class DurableJobExecutionRequest:
    job_id: str
    job_type: str
    job_version: int
    attempt: int
    requested_by: str
    input: dict
    cancellation_requested: Callable[[], Awaitable[bool]]

    def __post_init__(self) -> None:
        if (
            not self.job_id
            or not self.job_type
            or type(self.job_version) is not int
            or self.job_version < 1
            or type(self.attempt) is not int
            or self.attempt < 1
            or not self.requested_by
            or not isinstance(self.input, dict)
            or not callable(self.cancellation_requested)
        ):
            raise ValueError("Durable job execution request is invalid")

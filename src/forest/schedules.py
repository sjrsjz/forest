"""Bounded calendar schedules: UTC occurrences, explicit IANA timezone and DST.

Cron supports five numeric fields, lists, ranges and steps. Day-of-month and
day-of-week use cron's OR rule when both are restricted. Nonexistent wall times
are skipped; ambiguous times use the explicitly configured fold policy.
"""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from functools import lru_cache
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from forest.errors import ForestError


def aware(value: str | datetime) -> datetime:
    result = (
        datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    )
    if not isinstance(result, datetime) or result.utcoffset() is None:
        raise ValueError("Use an ISO timestamp with an explicit UTC offset")
    return result.astimezone(UTC)


@lru_cache(maxsize=256)
def parse_cron(expression: str) -> tuple:
    if not isinstance(expression, str) or len(expression) > 128:
        raise ValueError("Cron expression must be bounded text")
    fields = expression.split()
    if len(fields) != 5:
        raise ValueError("Cron requires minute hour day-of-month month weekday")
    parsed = []
    for field, minimum, maximum in zip(fields, (0, 0, 1, 1, 0), (59, 23, 31, 12, 6), strict=True):
        values = set()
        if len(field.split(",")) > 64:
            raise ValueError("Cron field contains too many alternatives")
        for part in field.split(","):
            pair = part.split("/")
            if len(pair) > 2 or (
                len(pair) == 2 and (not pair[1].isascii() or not pair[1].isdigit())
            ):
                raise ValueError("Invalid cron step")
            step = int(pair[1]) if len(pair) == 2 else 1
            if not 1 <= step <= maximum - minimum + 1:
                raise ValueError("Cron step is outside the field range")
            base = pair[0]
            if base == "*":
                left, right = minimum, maximum
            else:
                ends = base.split("-")
                if len(ends) > 2 or any(not v.isascii() or not v.isdigit() for v in ends):
                    raise ValueError("Cron accepts numeric values, ranges, lists and steps")
                left = int(ends[0])
                right = int(ends[1]) if len(ends) == 2 else maximum if len(pair) == 2 else left
                if not minimum <= left <= right <= maximum:
                    raise ValueError("Cron value is outside the field range")
            values.update(range(left, right + 1, step))
        parsed.append(tuple(sorted(values)))
    return (*parsed, fields[2].startswith("*"), fields[4].startswith("*"))


class ScheduleSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["interval", "cron"]
    timezone: str = Field(default="UTC", min_length=1, max_length=128)
    start_at: str
    end_at: str | None = None
    interval_seconds: int | None = Field(default=None, ge=1, le=31536000, strict=True)
    cron: str | None = Field(default=None, max_length=128)
    dst_fold: Literal["first", "second", "both"] = "first"

    @field_validator("timezone")
    @classmethod
    def zone(cls, value):
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError):
            raise ValueError("Unknown IANA timezone or missing timezone database") from None
        return value

    @field_validator("start_at", "end_at")
    @classmethod
    def timestamp(cls, value):
        return None if value is None else aware(value).isoformat()

    @model_validator(mode="after")
    def coherent(self):
        if self.end_at is not None and aware(self.end_at) <= aware(self.start_at):
            raise ValueError("Schedule end must follow its start")
        if self.kind == "interval":
            if self.interval_seconds is None or self.cron is not None:
                raise ValueError("Interval schedules require only interval_seconds")
        else:
            if self.cron is None or self.interval_seconds is not None:
                raise ValueError("Cron schedules require only cron")
            parse_cron(self.cron)
        return self


class JobSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=200)
    enabled: bool = True
    kind: Literal["conversation.trigger"] = "conversation.trigger"
    conversation_id: str = Field(min_length=1, max_length=256)
    prompt: str = Field(min_length=1, max_length=16000)
    schedule: ScheduleSpec
    misfire: Literal["skip", "coalesce", "catch_up"] = "coalesce"
    misfire_grace_seconds: int = Field(default=60, ge=0, le=3600, strict=True)
    catch_up_limit: int = Field(default=3, ge=1, le=10, strict=True)
    max_overlap: int = Field(default=1, ge=1, le=8, strict=True)
    delegation_expires_at: str
    max_runs: int = Field(default=1000, ge=1, le=100000, strict=True)

    @field_validator("prompt")
    @classmethod
    def bounded_prompt(cls, value):
        if not value.strip() or len(value.encode()) > 32768:
            raise ValueError("Prompt must be nonempty and at most 32768 UTF-8 bytes")
        return value

    @field_validator("delegation_expires_at")
    @classmethod
    def expiry(cls, value):
        return aware(value).isoformat()

    @model_validator(mode="after")
    def lifetime(self):
        if aware(self.delegation_expires_at) <= aware(self.schedule.start_at):
            raise ValueError("Delegation expiry must follow schedule start")
        return self


def _calendar(spec: ScheduleSpec, boundary: datetime, reverse: bool):
    minutes, hours, dom, months, dow, dom_star, dow_star = parse_cron(spec.cron)
    zone = ZoneInfo(spec.timezone)
    day = boundary.astimezone(zone).date()
    # Eight years cover Gregorian century exceptions (2096 -> 2104).
    for offset in range(366 * 8):
        current = day + timedelta(days=-offset if reverse else offset)
        if current.month not in months:
            continue
        day_match = current.day in dom
        weekday_match = (current.weekday() + 1) % 7 in dow
        if not (
            (day_match and weekday_match) if dom_star or dow_star else (day_match or weekday_match)
        ):
            continue
        candidates = set()
        for hour in hours:
            for minute in minutes:
                wall = datetime.combine(current, time(hour, minute))
                choices = []
                for fold in (0, 1):
                    local = wall.replace(tzinfo=zone, fold=fold)
                    instant = local.astimezone(UTC)
                    back = instant.astimezone(zone)
                    if back.replace(tzinfo=None) == wall and back.fold == fold:
                        choices.append(instant)
                if len(choices) == 2 and spec.dst_fold != "both":
                    choices = [choices[0 if spec.dst_fold == "first" else 1]]
                candidates.update(choices)
        for instant in sorted(candidates, reverse=reverse):
            if instant <= boundary if reverse else instant > boundary:
                yield instant


def next_occurrence(spec: ScheduleSpec | dict, after: datetime) -> datetime | None:
    spec = ScheduleSpec.model_validate(spec)
    after, start = aware(after), aware(spec.start_at)
    end = aware(spec.end_at) if spec.end_at else None
    if spec.kind == "interval":
        count = max(0, int((after - start).total_seconds() // spec.interval_seconds) + 1)
        result = start + timedelta(seconds=count * spec.interval_seconds)
    else:
        boundary = max(after, start - timedelta(microseconds=1))
        result = next(_calendar(spec, boundary, False), None)
        if result is None:
            raise ForestError(
                "SCHEDULE_HORIZON_EXCEEDED", "No cron occurrence within eight years", 422
            )
    return None if end is not None and result >= end else result


def previous_occurrence(spec: ScheduleSpec, at: datetime) -> datetime | None:
    at, start = aware(at), aware(spec.start_at)
    if spec.end_at:
        at = min(at, aware(spec.end_at) - timedelta(microseconds=1))
    if at < start:
        return None
    if spec.kind == "interval":
        count = int((at - start).total_seconds() // spec.interval_seconds)
        return start + timedelta(seconds=count * spec.interval_seconds)
    result = next(_calendar(spec, at, True), None)
    return result if result is not None and result >= start else None


def expand_schedule(values: JobSettings | dict, next_due: datetime, now: datetime) -> dict:
    """At most ten old occurrences per recovery window; cursor skips the rest.

    Normal lateness within grace is coalesced to its last due slot. A misfire
    chooses skip/one/latest-N explicitly, then advances beyond this captured now.
    No number of repeated scans can turn one bounded recovery into unbounded catch-up.
    """
    values = JobSettings.model_validate(values)
    next_due, now = aware(next_due), aware(now)
    end = min(now, aware(values.delegation_expires_at) - timedelta(microseconds=1))
    if next_due > end:
        return {"occurrences": [], "next_due_at": next_due, "misfired": False}
    latest = previous_occurrence(values.schedule, end)
    if latest is None or latest < next_due:
        return {"occurrences": [], "next_due_at": None, "misfired": False}
    misfired = (now - next_due).total_seconds() > values.misfire_grace_seconds
    occurrences = []
    if not misfired or values.misfire == "coalesce":
        occurrences = [latest]
    elif values.misfire == "catch_up":
        cursor = latest
        for _ in range(values.catch_up_limit):
            if cursor is None or cursor < next_due:
                break
            occurrences.append(cursor)
            cursor = previous_occurrence(values.schedule, cursor - timedelta(microseconds=1))
        occurrences.reverse()
    future = next_occurrence(values.schedule, end)
    if future is not None and future >= aware(values.delegation_expires_at):
        future = None
    return {"occurrences": occurrences, "next_due_at": future, "misfired": misfired}


def preview(spec: ScheduleSpec | dict, after: datetime, *, limit: int = 5) -> list[str]:
    if type(limit) is not int or not 1 <= limit <= 10:
        raise ValueError("Preview limit must be between one and ten")
    result = []
    for _ in range(limit):
        value = next_occurrence(spec, after)
        if value is None:
            break
        result.append(value.isoformat())
        after = value
    return result

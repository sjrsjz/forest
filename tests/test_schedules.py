"""Calendar behavior is explicit around DST and bounded after long downtime."""

from datetime import timedelta

import pytest

from forest.errors import ForestError
from forest.schedules import (
    JobSettings,
    ScheduleSpec,
    aware,
    expand_schedule,
    next_occurrence,
    parse_cron,
    preview,
)


def cron(expression, **changes):
    return ScheduleSpec(kind="cron", cron=expression, start_at="2026-01-01T00:00:00Z", **changes)


def test_interval_is_elapsed_utc_and_end_exclusive():
    spec = ScheduleSpec(
        kind="interval",
        interval_seconds=3600,
        timezone="America/New_York",
        start_at="2026-03-08T01:30:00-05:00",
        end_at="2026-03-08T04:30:00-04:00",
    )
    assert preview(spec, aware("2026-03-08T06:00Z")) == [
        "2026-03-08T06:30:00+00:00",
        "2026-03-08T07:30:00+00:00",
    ]


def test_cron_skips_nonexistent_wall_time():
    spec = cron("30 2 * * *", timezone="America/New_York")
    assert next_occurrence(spec, aware("2026-03-08T00:00Z")) == aware("2026-03-09T06:30Z")


@pytest.mark.parametrize(
    ("fold", "expected"),
    [
        ("first", ["2026-11-01T05:30:00+00:00", "2026-11-02T06:30:00+00:00"]),
        ("second", ["2026-11-01T06:30:00+00:00", "2026-11-02T06:30:00+00:00"]),
        ("both", ["2026-11-01T05:30:00+00:00", "2026-11-01T06:30:00+00:00"]),
    ],
)
def test_cron_ambiguous_fold_is_explicit(fold, expected):
    assert (
        preview(
            cron("30 1 * * *", timezone="America/New_York", dst_fold=fold),
            aware("2026-11-01T00:00Z"),
            limit=2,
        )
        == expected
    )


def test_cron_restricted_day_fields_use_or_and_utc_order():
    assert preview(cron("0 9 13 * 1"), aware("2026-09-12T00:00Z"), limit=2) == [
        "2026-09-13T09:00:00+00:00",
        "2026-09-14T09:00:00+00:00",
    ]
    assert next_occurrence(cron("*/15 8-9 * * 1-5"), aware("2026-09-11T09:50Z")) == aware(
        "2026-09-14T08:00Z"
    )


@pytest.mark.parametrize(
    "expression",
    [
        "* * * *",
        "61 * * * *",
        "0 24 * * *",
        "0 0 0 * *",
        "0 0 * * 7",
        "@daily",
        "*/0 * * * *",
        "1--2 * * * *",
        "x * * * *",
        "０ * * * *",
    ],
)
def test_reject_invalid_cron(expression):
    with pytest.raises(ValueError):
        parse_cron(expression)


def test_impossible_cron_stops_at_bounded_horizon():
    with pytest.raises(ForestError, match="eight years"):
        next_occurrence(cron("0 0 31 2 *"), aware("2026-01-01T00:00Z"))


def test_century_leap_exception_has_eight_year_successor():
    assert next_occurrence(cron("0 0 29 2 *"), aware("2096-02-29T00:00Z")) == aware(
        "2104-02-29T00:00Z"
    )


@pytest.mark.parametrize(
    ("policy", "offsets"), [("skip", []), ("coalesce", [0]), ("catch_up", [-2, -1, 0])]
)
def test_long_outage_has_one_bounded_recovery_window(policy, offsets):
    start, now = aware("2020-01-01T00:00Z"), aware("2026-09-13T00:00Z")
    values = JobSettings(
        name="job",
        conversation_id="conversation",
        prompt="reminder",
        schedule={"kind": "interval", "interval_seconds": 1, "start_at": start.isoformat()},
        misfire=policy,
        misfire_grace_seconds=0,
        catch_up_limit=3,
        delegation_expires_at="2027-01-01T00:00Z",
    )
    result = expand_schedule(values, start, now)
    assert result["occurrences"] == [now + timedelta(seconds=n) for n in offsets]
    assert result["next_due_at"] == now + timedelta(seconds=1)
    assert expand_schedule(values, result["next_due_at"], now)["occurrences"] == []


@pytest.mark.parametrize(
    "changes",
    [
        {"start_at": "2026-01-01"},
        {"timezone": "../UTC"},
        {"interval_seconds": True},
        {"end_at": "2025-01-01T00:00Z"},
    ],
)
def test_timezone_offsets_and_strict_bounds(changes):
    with pytest.raises(ValueError):
        ScheduleSpec.model_validate(
            {"kind": "interval", "interval_seconds": 60, "start_at": "2026-01-01T00:00Z", **changes}
        )

"""Unit coverage (no database) for the §6.7 schedule building blocks:
``ScheduleSpec`` cron compilation and the schedule ``context`` round-trip.
"""

from datetime import datetime, timedelta, timezone

import pytest

from temporal_dbos._internal import conversion, schedules
from temporal_dbos._schedule import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleCalendarSpec,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
    _schedule_from_context,
    compile_spec,
    serialize_schedule_context,
)


@pytest.fixture(autouse=True)
def _default_converter() -> None:
    conversion.reset_converter()


@pytest.mark.parametrize(
    "every,expected",
    [
        (timedelta(seconds=30), "*/30 * * * * *"),
        (timedelta(minutes=2), "*/2 * * * *"),
        (timedelta(minutes=1), "*/1 * * * *"),
        (timedelta(minutes=60), "0 * * * *"),
        (timedelta(hours=2), "0 */2 * * *"),
        (timedelta(hours=24), "0 0 * * *"),
    ],
)
def test_interval_to_cron_exact(every: timedelta, expected: str) -> None:
    assert schedules.interval_to_cron(every) == expected
    # Every compiled cron must be valid for DBOS's croniter.
    schedules.validate_cron(schedules.interval_to_cron(every))


def test_interval_to_cron_non_divisible_approximates() -> None:
    # 7 minutes doesn't divide 60; approximated (still a valid cron).
    cron = schedules.interval_to_cron(timedelta(minutes=7))
    schedules.validate_cron(cron)


def test_interval_to_cron_rejects_nonpositive() -> None:
    with pytest.raises(ValueError):
        schedules.interval_to_cron(timedelta(0))


def test_calendar_to_cron_daily_default() -> None:
    cron = schedules.calendar_to_cron(
        second=[(0, 0, 1)],
        minute=[(0, 0, 1)],
        hour=[(0, 0, 1)],
        day_of_month=[(1, 31, 1)],
        month=[(1, 12, 1)],
        day_of_week=[(0, 6, 1)],
        year=[],
    )
    assert cron == "0 0 * * *"


def test_calendar_to_cron_specific_fields() -> None:
    cron = schedules.calendar_to_cron(
        second=[(0, 0, 1)],
        minute=[(30, 30, 1)],
        hour=[(9, 17, 1)],
        day_of_month=[(1, 31, 1)],
        month=[(1, 12, 1)],
        day_of_week=[(1, 5, 1)],
        year=[],
    )
    assert cron == "30 9-17 * * 1-5"
    schedules.validate_cron(cron)


def test_compile_spec_prefers_cron_then_interval() -> None:
    cron, tz = compile_spec(ScheduleSpec(cron_expressions=["*/5 * * * *"]))
    assert cron == "*/5 * * * *"
    cron, tz = compile_spec(
        ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(minutes=2))])
    )
    assert cron == "*/2 * * * *"


def test_compile_spec_timezone() -> None:
    cron, tz = compile_spec(
        ScheduleSpec(
            intervals=[ScheduleIntervalSpec(every=timedelta(minutes=2))],
            time_zone_name="America/New_York",
        )
    )
    assert tz == "America/New_York"


def test_compile_spec_requires_a_spec() -> None:
    with pytest.raises(ValueError):
        compile_spec(ScheduleSpec())


def test_context_round_trip() -> None:
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            "MyWorkflow",
            "the-arg",
            id="wf-id",
            task_queue="tq",
            run_timeout=timedelta(seconds=30),
        ),
        spec=ScheduleSpec(
            intervals=[ScheduleIntervalSpec(every=timedelta(minutes=2))],
            calendars=[ScheduleCalendarSpec(minute=(ScheduleRange(0, 30, 15),))],
            start_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            jitter=timedelta(seconds=5),
            time_zone_name="UTC",
        ),
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.ALLOW_ALL),
        state=ScheduleState(note="hello", paused=True),
    )
    ctx = serialize_schedule_context(schedule)
    # The action args must be encoded payloads (JSON-safe for DBOS transport).
    assert isinstance(ctx["action"]["args"], list)

    rebuilt = _schedule_from_context(ctx)
    action = rebuilt.action
    assert isinstance(action, ScheduleActionStartWorkflow)
    assert action.workflow == "MyWorkflow"
    assert list(action.args) == ["the-arg"]
    assert action.id == "wf-id"
    assert action.task_queue == "tq"
    assert action.run_timeout == timedelta(seconds=30)
    assert rebuilt.spec.intervals[0].every == timedelta(minutes=2)
    assert rebuilt.spec.calendars[0].minute[0] == ScheduleRange(0, 30, 15)
    assert rebuilt.spec.start_at == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert rebuilt.spec.jitter == timedelta(seconds=5)
    assert rebuilt.policy.overlap == ScheduleOverlapPolicy.ALLOW_ALL
    assert rebuilt.state.note == "hello"


def test_action_requires_id_and_task_queue() -> None:
    with pytest.raises(ValueError):
        ScheduleActionStartWorkflow("W", task_queue="tq")
    with pytest.raises(ValueError):
        ScheduleActionStartWorkflow("W", id="x")


def test_schedule_range_post_init_defaults() -> None:
    r = ScheduleRange(5)
    assert r.end == 5 and r.step == 1
    r = ScheduleRange(1, 10)
    assert r.step == 1

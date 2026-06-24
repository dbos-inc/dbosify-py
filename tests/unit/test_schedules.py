"""Unit coverage (no database) for the schedule building blocks:
``ScheduleSpec`` cron compilation and the schedule ``context`` round-trip.
"""

from datetime import datetime, timedelta, timezone

import pytest

from dbosify._internal import conversion, schedules
from dbosify._schedule import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleCalendarSpec,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
    _next_action_times,
    _schedule_from_context,
    compile_spec,
    encode_action_attributes,
    require_overlap_override_supported,
    require_supported_overlap,
    serialize_schedule_context,
)
from dbosify.common import (
    Priority,
    RetryPolicy,
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
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


def test_next_action_times_honor_schedule_timezone() -> None:
    # A 09:00 daily calendar in America/New_York must report next fire times in
    # UTC at 13:00/14:00 (EDT/EST) — not a naive 09:00 read in UTC.
    schedule = Schedule(
        action=ScheduleActionStartWorkflow("W", id="w", task_queue="tq"),
        spec=ScheduleSpec(
            calendars=[ScheduleCalendarSpec(hour=(ScheduleRange(9),))],
            time_zone_name="America/New_York",
        ),
    )
    times = _next_action_times(serialize_schedule_context(schedule), 3)
    assert times
    for t in times:
        assert t.utcoffset() == timedelta(0)  # reported in UTC
        assert t.hour in (13, 14)  # 09:00 New York is 13:00 (EDT) / 14:00 (EST) UTC


def test_next_action_times_utc_schedule_unchanged() -> None:
    # A plain UTC schedule still reports the wall-clock hour (no regression).
    schedule = Schedule(
        action=ScheduleActionStartWorkflow("W", id="w", task_queue="tq"),
        spec=ScheduleSpec(calendars=[ScheduleCalendarSpec(hour=(ScheduleRange(9),))]),
    )
    times = _next_action_times(serialize_schedule_context(schedule), 1)
    assert times and times[0].hour == 9 and times[0].utcoffset() == timedelta(0)


async def test_context_round_trip() -> None:
    kw = SearchAttributeKey.for_keyword("RoundTripKw")
    start_action = ScheduleActionStartWorkflow(
        "MyWorkflow",
        "the-arg",
        id="wf-id",
        task_queue="tq",
        run_timeout=timedelta(seconds=30),
        retry_policy=RetryPolicy(maximum_attempts=5),
        memo={"team": "sched"},
        typed_search_attributes=TypedSearchAttributes([SearchAttributePair(kw, "v")]),
        static_summary="a summary",
        static_details="some details",
        priority=Priority(priority_key=3),
    )
    schedule = Schedule(
        action=start_action,
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
    # Memo + search attributes are encoded into the context as create_schedule
    # does, so the round-trip exercises the full stored form.
    encoded = await encode_action_attributes(start_action)
    assert encoded is not None
    ctx["action"]["attributes"] = encoded
    # The action args must be encoded payloads (JSON-safe for DBOS transport).
    assert isinstance(ctx["action"]["args"], list)

    rebuilt = await _schedule_from_context(ctx)
    action = rebuilt.action
    assert isinstance(action, ScheduleActionStartWorkflow)
    assert action.workflow == "MyWorkflow"
    assert list(action.args) == ["the-arg"]
    assert action.id == "wf-id"
    assert action.task_queue == "tq"
    assert action.run_timeout == timedelta(seconds=30)
    # Every action field round-trips losslessly.
    assert action.retry_policy == RetryPolicy(maximum_attempts=5)
    assert action.memo == {"team": "sched"}
    assert action.typed_search_attributes.get(kw) == "v"
    assert action.static_summary == "a summary"
    assert action.static_details == "some details"
    assert action.priority == Priority(priority_key=3)
    # Re-encoding the rebuilt action reproduces the stored attributes byte-for-
    # byte — proof the round-trip is stable (update() preserves everything).
    assert await encode_action_attributes(action) == encoded
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


@pytest.mark.parametrize(
    "policy",
    [
        None,
        ScheduleOverlapPolicy.SKIP,
        ScheduleOverlapPolicy.CANCEL_OTHER,
        ScheduleOverlapPolicy.TERMINATE_OTHER,
        ScheduleOverlapPolicy.ALLOW_ALL,
    ],
)
def test_require_supported_overlap_allows(policy: object) -> None:
    require_supported_overlap(policy)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "policy",
    [ScheduleOverlapPolicy.BUFFER_ONE, ScheduleOverlapPolicy.BUFFER_ALL],
)
def test_require_supported_overlap_rejects_buffer(
    policy: ScheduleOverlapPolicy,
) -> None:
    with pytest.raises(NotImplementedError):
        require_supported_overlap(policy)


def test_default_overlap_is_skip() -> None:
    # Matches Temporal's default (we honor SKIP).
    assert SchedulePolicy().overlap == ScheduleOverlapPolicy.SKIP


@pytest.mark.parametrize("override", [None, ScheduleOverlapPolicy.ALLOW_ALL])
def test_overlap_override_allows_none_and_allow_all(override: object) -> None:
    require_overlap_override_supported(override)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "override",
    [
        ScheduleOverlapPolicy.SKIP,
        ScheduleOverlapPolicy.CANCEL_OTHER,
        ScheduleOverlapPolicy.TERMINATE_OTHER,
        ScheduleOverlapPolicy.BUFFER_ONE,
    ],
)
def test_overlap_override_rejects_others(override: ScheduleOverlapPolicy) -> None:
    with pytest.raises(NotImplementedError):
        require_overlap_override_supported(override)

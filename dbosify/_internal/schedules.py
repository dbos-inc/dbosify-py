"""Cron-expression helpers (DESIGN §6.4 cron; §6.7 schedules build on this).

DBOS vendors a croniter; we reuse it so client-side fire computations and the
DBOS worker-side scheduler agree on semantics. Temporal cron strings are
5-field, evaluated in UTC unless prefixed with ``CRON_TZ=<IANA name>`` (or
``TZ=``, which Temporal also accepts). 6-field (leading seconds) and 7-field
(trailing year) expressions are accepted as an extension — Temporal rejects
them, but they are invaluable for fast tests and cost nothing to support.

§6.7 schedules compile a ``ScheduleSpec`` (intervals / calendars / cron
expressions) down to a single cron string + timezone for ``DBOS.create_schedule``.
Interval periods that don't divide a cron boundary evenly, calendar fields
beyond cron's expressiveness, and interval offsets are approximated with a
logged deviation (DEVIATIONS schedules).
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from dbos._croniter import croniter  # type: ignore[attr-defined]

logger = logging.getLogger("dbosify.schedules")

__all__ = [
    "next_fire_delay",
    "parse_cron",
    "validate_cron",
    "interval_to_cron",
    "calendar_to_cron",
]


def parse_cron(expr: str) -> Tuple[str, ZoneInfo]:
    """Split an optional ``CRON_TZ=``/``TZ=`` prefix off a cron expression.

    Raises ValueError for an unknown timezone or an invalid expression.
    """
    expr = expr.strip()
    tz = ZoneInfo("UTC")
    for prefix in ("CRON_TZ=", "TZ="):
        if expr.startswith(prefix):
            tz_name, _, rest = expr[len(prefix) :].partition(" ")
            try:
                tz = ZoneInfo(tz_name)
            except Exception:
                raise ValueError(
                    f"Unknown timezone in cron schedule: {tz_name!r}"
                ) from None
            expr = rest.strip()
            break
    if not croniter.is_valid(expr, second_at_beginning=True):
        raise ValueError(f"Invalid cron schedule: {expr!r}")
    return expr, tz


def validate_cron(expr: str) -> None:
    """Raise ValueError if ``expr`` is not a usable cron schedule."""
    parse_cron(expr)


def next_fire_delay(expr: str, now: datetime) -> float:
    """Seconds from ``now`` until the expression's next occurrence (>= 0)."""
    fields, tz = parse_cron(expr)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    it = croniter(fields, now.astimezone(tz), second_at_beginning=True)
    fire_at: datetime = it.get_next(datetime)
    return max(0.0, (fire_at - now).total_seconds())


def interval_to_cron(every: timedelta, offset: Optional[timedelta] = None) -> str:
    """Compile a ``ScheduleIntervalSpec`` to a cron string.

    Exact when the period divides a cron boundary evenly (seconds into 60,
    minutes into 60, hours into 24, or whole days); otherwise approximated to
    the nearest minute granularity with a logged deviation. A non-zero
    ``offset`` is not representable in cron and is dropped with a deviation —
    cron alignment is to wall-clock boundaries, not ``epoch + offset``.
    """
    total = every.total_seconds()
    if total <= 0:
        raise ValueError("ScheduleIntervalSpec.every must be positive")
    if offset is not None and offset.total_seconds():
        logger.debug(
            "schedule interval offset %s is not representable in cron; ignored "
            "(DEVIATIONS schedules)",
            offset,
        )
    secs = int(round(total))
    if secs < 60:
        if 60 % secs == 0:
            return f"*/{secs} * * * * *"
        _approx(every)
        return f"*/{secs} * * * * *"
    if secs % 60 == 0:
        mins = secs // 60
        if mins == 60:
            return "0 * * * *"
        if mins < 60 and 60 % mins == 0:
            return f"*/{mins} * * * *"
    if secs % 3600 == 0:
        hours = secs // 3600
        if hours == 24:
            return "0 0 * * *"
        if hours < 24 and 24 % hours == 0:
            return f"0 */{hours} * * *"
    if secs % 86400 == 0:
        days = secs // 86400
        _approx(every)
        return f"0 0 */{min(days, 31)} * *"
    _approx(every)
    mins = max(1, round(secs / 60))
    return f"*/{mins} * * * *" if mins < 60 else "0 * * * *"


def _approx(every: timedelta) -> None:
    logger.debug(
        "schedule interval every=%s does not divide a cron boundary evenly; "
        "approximated to the nearest cron expression (DEVIATIONS schedules)",
        every,
    )


def calendar_to_cron(
    *,
    second: Sequence[Tuple[int, int, int]],
    minute: Sequence[Tuple[int, int, int]],
    hour: Sequence[Tuple[int, int, int]],
    day_of_month: Sequence[Tuple[int, int, int]],
    month: Sequence[Tuple[int, int, int]],
    day_of_week: Sequence[Tuple[int, int, int]],
    year: Sequence[Tuple[int, int, int]],
) -> str:
    """Compile a ``ScheduleCalendarSpec`` (each field a sequence of
    ``(start, end, step)`` ranges) to a cron string.

    Emits a 6-field (seconds-first) expression when any second range is
    non-default, else 5-field. The ``year`` field has no cron equivalent and
    is dropped with a deviation when constraining (DEVIATIONS schedules).
    """
    if year:
        logger.debug(
            "schedule calendar year constraint is not representable in cron; "
            "ignored (DEVIATIONS schedules)",
        )
    sec_field = _ranges_to_field(second, 0, 59)
    fields: List[str] = [
        _ranges_to_field(minute, 0, 59),
        _ranges_to_field(hour, 0, 23),
        _ranges_to_field(day_of_month, 1, 31),
        _ranges_to_field(month, 1, 12),
        _ranges_to_field(day_of_week, 0, 6),
    ]
    if sec_field != "0":
        return " ".join([sec_field, *fields])
    return " ".join(fields)


def _ranges_to_field(
    ranges: Sequence[Tuple[int, int, int]], domain_lo: int, domain_hi: int
) -> str:
    """Render one calendar field's ranges as a cron token."""
    if not ranges:
        return "*"
    tokens: List[str] = []
    for start, end, step in ranges:
        end = end if end >= start else start
        step = step if step > 0 else 1
        if start <= domain_lo and end >= domain_hi and step == 1:
            tokens.append("*")
        elif start == end:
            tokens.append(str(start))
        elif step == 1:
            tokens.append(f"{start}-{end}")
        else:
            tokens.append(f"{start}-{end}/{step}")
    if "*" in tokens:
        return "*"
    return ",".join(tokens)

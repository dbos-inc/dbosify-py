"""Cron-expression helpers (DESIGN §6.4 cron; §6.7 schedules build on this).

DBOS vendors a croniter; we reuse it so client-side fire computations and the
DBOS worker-side scheduler agree on semantics. Temporal cron strings are
5-field, evaluated in UTC unless prefixed with ``CRON_TZ=<IANA name>`` (or
``TZ=``, which Temporal also accepts). 6-field (leading seconds) and 7-field
(trailing year) expressions are accepted as an extension — Temporal rejects
them, but they are invaluable for fast tests and cost nothing to support.
"""

from datetime import datetime, timezone
from typing import Tuple
from zoneinfo import ZoneInfo

from dbos._croniter import croniter  # type: ignore[attr-defined]

__all__ = ["next_fire_delay", "parse_cron", "validate_cron"]


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

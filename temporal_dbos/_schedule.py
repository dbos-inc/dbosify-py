"""Schedules (DESIGN §6.7): the temporalio-shaped ``Schedule`` type surface
and ``ScheduleHandle``, backed by DBOS schedule primitives.

Mirrors ``temporalio.client``'s schedule types (re-exported from
``temporal_dbos.client``). A Temporal ``Schedule`` compiles to a single DBOS
schedule row: ``DBOS.create_schedule`` fires a generic dispatcher workflow
(``__temporal_schedule_fire``) which, at each occurrence, starts the action
workflow with a per-occurrence deterministic id (see
``_internal/dispatcher.py``). The action and the full ``ScheduleSpec`` ride in
the schedule's ``context`` so ``describe``/``list``/``update`` can reconstruct
it; ``ScheduleSpec`` itself compiles down to a cron string + timezone for DBOS.

Deviations (DEVIATIONS D22): interval periods that don't divide a cron
boundary, calendar ``year`` constraints, and interval offsets are approximated;
overlap policy honors SKIP / CANCEL_OTHER / TERMINATE_OTHER / ALLOW_ALL (via a
bounded backward walk of prior occurrences at fire time) but rejects
BUFFER_ONE/BUFFER_ALL; ``update`` is delete-then-recreate (DBOS has no in-place
schedule update); schedule history (recent_actions) is not tracked.
"""

import asyncio
import inspect
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import IntEnum
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
)

from ._internal import conversion
from ._internal import registry as _registry
from ._internal import schedules as _schedules
from ._internal.payloads import serialize_retry_policy
from .common import RetryPolicy

if TYPE_CHECKING:
    from .client import Client

__all__ = [
    "ScheduleAction",
    "ScheduleActionExecution",
    "ScheduleActionExecutionStartWorkflow",
    "ScheduleActionResult",
    "ScheduleActionStartWorkflow",
    "ScheduleAsyncIterator",
    "ScheduleBackfill",
    "ScheduleCalendarSpec",
    "ScheduleDescription",
    "ScheduleHandle",
    "ScheduleInfo",
    "ScheduleIntervalSpec",
    "ScheduleListAction",
    "ScheduleListActionStartWorkflow",
    "ScheduleListDescription",
    "ScheduleListInfo",
    "ScheduleListSchedule",
    "ScheduleListState",
    "ScheduleOverlapPolicy",
    "SchedulePolicy",
    "ScheduleRange",
    "ScheduleSpec",
    "ScheduleState",
    "ScheduleUpdate",
    "ScheduleUpdateInput",
]

# The DBOS workflow name of the generic schedule-fire dispatcher (registered by
# the Worker; defined in _internal/dispatcher.py).
SCHEDULE_FIRE_WORKFLOW = "__temporal_schedule_fire"

# Sentinel for the optional positional ``arg`` (vs an explicit ``args=``).
_arg_unset = object()


# ---------------------------------------------------------------------------
# Spec types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScheduleRange:
    """Inclusive range of integers for a calendar field."""

    start: int
    end: int = 0
    step: int = 0

    def __post_init__(self) -> None:
        # Mirror temporalio: end defaults to start, step to 1.
        if self.end < self.start:
            object.__setattr__(self, "end", self.start)
        if self.step == 0:
            object.__setattr__(self, "step", 1)


@dataclass
class ScheduleCalendarSpec:
    """Calendar-based specification of times (cron-like fields)."""

    second: Sequence[ScheduleRange] = (ScheduleRange(0),)
    minute: Sequence[ScheduleRange] = (ScheduleRange(0),)
    hour: Sequence[ScheduleRange] = (ScheduleRange(0),)
    day_of_month: Sequence[ScheduleRange] = (ScheduleRange(1, 31),)
    month: Sequence[ScheduleRange] = (ScheduleRange(1, 12),)
    year: Sequence[ScheduleRange] = ()
    day_of_week: Sequence[ScheduleRange] = (ScheduleRange(0, 6),)
    comment: Optional[str] = None


@dataclass
class ScheduleIntervalSpec:
    """Interval-based specification of times."""

    every: timedelta
    offset: Optional[timedelta] = None


@dataclass
class ScheduleSpec:
    """Specification of times a schedule's action should occur."""

    calendars: Sequence[ScheduleCalendarSpec] = field(default_factory=list)
    intervals: Sequence[ScheduleIntervalSpec] = field(default_factory=list)
    cron_expressions: Sequence[str] = field(default_factory=list)
    skip: Sequence[ScheduleCalendarSpec] = field(default_factory=list)
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None
    jitter: Optional[timedelta] = None
    time_zone_name: Optional[str] = None


# ---------------------------------------------------------------------------
# Policy / state
# ---------------------------------------------------------------------------


class ScheduleOverlapPolicy(IntEnum):
    """What happens when a workflow would start but one is already running.

    Values mirror Temporal's ``ScheduleOverlapPolicy`` proto.
    """

    SKIP = 1
    BUFFER_ONE = 2
    BUFFER_ALL = 3
    CANCEL_OTHER = 4
    TERMINATE_OTHER = 5
    ALLOW_ALL = 6


@dataclass
class SchedulePolicy:
    """Policies of a schedule.

    ``overlap`` defaults to ``SKIP`` (matching Temporal). SKIP, CANCEL_OTHER,
    TERMINATE_OTHER, and ALLOW_ALL are honored; BUFFER_ONE/BUFFER_ALL are
    rejected at ``create_schedule`` time (DEVIATIONS D22)."""

    overlap: ScheduleOverlapPolicy = field(
        default_factory=lambda: ScheduleOverlapPolicy.SKIP
    )
    catchup_window: timedelta = timedelta(days=365)
    pause_on_failure: bool = False


@dataclass
class ScheduleState:
    """State of a schedule."""

    note: Optional[str] = None
    paused: bool = False
    limited_actions: bool = False
    remaining_actions: int = 0


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------


class ScheduleAction:
    """Base class for an action a schedule can take."""


class ScheduleActionStartWorkflow(ScheduleAction):
    """Schedule action to start a workflow."""

    def __init__(
        self,
        workflow: Union[str, Callable[..., Awaitable[Any]]],
        arg: Any = _arg_unset,
        *,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        task_queue: Optional[str] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        retry_policy: Optional[RetryPolicy] = None,
        memo: Optional[Mapping[str, Any]] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        priority: Optional[Any] = None,
    ) -> None:
        super().__init__()
        if id is None:
            raise ValueError("ScheduleActionStartWorkflow requires an id")
        if task_queue is None:
            raise ValueError("ScheduleActionStartWorkflow requires a task_queue")
        self.workflow = workflow
        self.args: Sequence[Any] = _resolve_args(arg, args)
        self.id = id
        self.task_queue = task_queue
        self.execution_timeout = execution_timeout
        self.run_timeout = run_timeout
        self.task_timeout = task_timeout
        self.retry_policy = retry_policy
        self.memo = memo
        self.static_summary = static_summary
        self.static_details = static_details
        self.priority = priority


class ScheduleActionExecution:
    """Base class for an action execution."""


@dataclass
class ScheduleActionExecutionStartWorkflow(ScheduleActionExecution):
    """Execution of a scheduled workflow start."""

    workflow_id: str
    first_execution_run_id: str


@dataclass
class ScheduleActionResult:
    """Information about a single schedule action execution."""

    scheduled_at: datetime
    started_at: datetime
    action: ScheduleActionExecution


# ---------------------------------------------------------------------------
# Top-level schedule
# ---------------------------------------------------------------------------


@dataclass
class Schedule:
    """A schedule for periodically running an action."""

    action: ScheduleAction
    spec: ScheduleSpec
    policy: SchedulePolicy = field(default_factory=SchedulePolicy)
    state: ScheduleState = field(default_factory=ScheduleState)


@dataclass
class ScheduleBackfill:
    """Time period and policy for backfilling a schedule."""

    start_at: datetime
    end_at: datetime
    overlap: Optional[ScheduleOverlapPolicy] = None


# ---------------------------------------------------------------------------
# Describe / info
# ---------------------------------------------------------------------------


@dataclass
class ScheduleInfo:
    """Information about a schedule."""

    num_actions: int
    num_actions_missed_catchup_window: int
    num_actions_skipped_overlap: int
    running_actions: Sequence[ScheduleActionExecution]
    recent_actions: Sequence[ScheduleActionResult]
    next_action_times: Sequence[datetime]
    created_at: datetime
    last_updated_at: Optional[datetime]


@dataclass
class ScheduleDescription:
    """Description of a schedule."""

    id: str
    schedule: Schedule
    info: ScheduleInfo


@dataclass
class ScheduleUpdateInput:
    """Parameter passed to a schedule updater callback."""

    description: ScheduleDescription


@dataclass
class ScheduleUpdate:
    """Result of a schedule updater callback."""

    schedule: Schedule
    search_attributes: Optional[Any] = None


# ---------------------------------------------------------------------------
# List types
# ---------------------------------------------------------------------------


class ScheduleListAction:
    """Base class for an action a listed schedule can take."""


@dataclass
class ScheduleListActionStartWorkflow(ScheduleListAction):
    """Action to start a workflow on a listed schedule."""

    workflow: str


@dataclass
class ScheduleListInfo:
    """Information about a listed schedule."""

    recent_actions: Sequence[ScheduleActionResult]
    next_action_times: Sequence[datetime]


@dataclass
class ScheduleListState:
    """State of a listed schedule."""

    note: Optional[str]
    paused: bool


@dataclass
class ScheduleListSchedule:
    """Details of a listed schedule."""

    action: ScheduleListAction
    spec: ScheduleSpec
    state: ScheduleListState


@dataclass
class ScheduleListDescription:
    """Description of a listed schedule."""

    id: str
    schedule: Optional[ScheduleListSchedule]
    info: Optional[ScheduleListInfo]


class ScheduleAsyncIterator:
    """Async iterator over ``ScheduleListDescription`` (the result of
    :py:meth:`Client.list_schedules`).

    Construction differs from temporalio (it wraps a pre-fetched page rather
    than a gRPC paginator); the iteration contract is the same.
    """

    def __init__(self, page: Sequence[ScheduleListDescription]) -> None:
        self._page = list(page)
        self._index = 0

    @property
    def current_page_index(self) -> int:
        return self._index

    @property
    def current_page(self) -> Optional[Sequence[ScheduleListDescription]]:
        return self._page

    @property
    def next_page_token(self) -> Optional[bytes]:
        return None

    async def fetch_next_page(self, *, page_size: Optional[int] = None) -> None:
        # Single-page implementation: nothing more to fetch.
        return None

    def __aiter__(self) -> "ScheduleAsyncIterator":
        return self

    async def __anext__(self) -> ScheduleListDescription:
        if self._index >= len(self._page):
            raise StopAsyncIteration
        item = self._page[self._index]
        self._index += 1
        return item


# ---------------------------------------------------------------------------
# Handle
# ---------------------------------------------------------------------------


class ScheduleHandle:
    """A handle for a schedule, used to perform actions on it."""

    def __init__(self, client: "Client", id: str) -> None:
        self._client = client
        self.id = id

    async def describe(
        self,
        *,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> ScheduleDescription:
        """Fetch this schedule's description."""
        row = await self._client._dbos_client.get_schedule_async(self.id)
        if row is None:
            raise RuntimeError(f"Schedule {self.id!r} not found")
        return _description_from_row(row)

    async def update(
        self,
        updater: Callable[
            [ScheduleUpdateInput],
            Union[Optional[ScheduleUpdate], Awaitable[Optional[ScheduleUpdate]]],
        ],
        *,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Update this schedule. The ``updater`` (sync or async) receives the
        current description and returns the new ``ScheduleUpdate`` (or ``None``
        to skip). Implemented as delete-then-recreate (DEVIATIONS D22)."""
        desc = await self.describe()
        outcome = updater(ScheduleUpdateInput(description=desc))
        if inspect.isawaitable(outcome):
            outcome = await outcome
        if outcome is None:
            return
        await _replace_schedule(self._client, self.id, outcome.schedule)

    async def pause(
        self,
        *,
        note: Optional[str] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Pause this schedule (DBOS ``pause_schedule``). ``note`` is accepted
        but not persisted (DEVIATIONS D22)."""
        await asyncio.to_thread(self._client._dbos_client.pause_schedule, self.id)

    async def unpause(
        self,
        *,
        note: Optional[str] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Unpause this schedule (DBOS ``resume_schedule``). ``note`` is accepted
        but not persisted (DEVIATIONS D22)."""
        await asyncio.to_thread(self._client._dbos_client.resume_schedule, self.id)

    async def trigger(
        self,
        *,
        overlap: Optional[ScheduleOverlapPolicy] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Trigger an immediate action on this schedule. The action runs under
        the schedule's configured overlap policy; a per-call ``overlap`` override
        is accepted only as ``ALLOW_ALL`` (others raise — DEVIATIONS D22)."""
        require_overlap_override_supported(overlap)
        await asyncio.to_thread(self._client._dbos_client.trigger_schedule, self.id)

    async def backfill(
        self,
        *backfill: ScheduleBackfill,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Backfill this schedule over the given time periods. Backfilled actions
        run under the schedule's configured overlap policy; a per-backfill
        ``overlap`` override is accepted only as ``ALLOW_ALL`` (others raise —
        DEVIATIONS D22)."""
        if not backfill:
            raise ValueError("At least one backfill required")
        for b in backfill:
            require_overlap_override_supported(b.overlap)
        for b in backfill:
            await asyncio.to_thread(
                self._client._dbos_client.backfill_schedule,
                self.id,
                b.start_at,
                b.end_at,
            )

    async def delete(
        self,
        *,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Delete this schedule."""
        await self._client._dbos_client.delete_schedule_async(self.id)


# ---------------------------------------------------------------------------
# Context (de)serialization — the schedule's DBOS ``context`` payload. The
# fire dispatcher reads ``context["action"]`` directly (see dispatcher.py).
# ---------------------------------------------------------------------------


def _resolve_args(arg: Any, args: Sequence[Any]) -> List[Any]:
    if arg is not _arg_unset:
        if args:
            raise ValueError("Cannot have both arg and args")
        return [arg]
    return list(args)


def _secs(td: Optional[timedelta]) -> Optional[float]:
    return td.total_seconds() if td is not None else None


def _td(secs: Optional[float]) -> Optional[timedelta]:
    return timedelta(seconds=secs) if secs is not None else None


def _resolve_type_name(workflow: Union[str, Callable[..., Any]]) -> str:
    if isinstance(workflow, str):
        return workflow
    if isinstance(workflow, type):
        return _registry.workflow_definition_of(workflow).name
    name = getattr(workflow, _registry.WORKFLOW_NAME_ATTR, None)
    if name is not None:
        return str(name)
    raise TypeError(
        f"Cannot resolve a workflow type from {workflow!r}: pass the "
        "@workflow.defn class, its @workflow.run method, or the type name"
    )


def _serialize_calendar(cal: ScheduleCalendarSpec) -> Dict[str, Any]:
    def ranges(rs: Sequence[ScheduleRange]) -> List[List[int]]:
        return [[r.start, r.end, r.step] for r in rs]

    return {
        "second": ranges(cal.second),
        "minute": ranges(cal.minute),
        "hour": ranges(cal.hour),
        "day_of_month": ranges(cal.day_of_month),
        "month": ranges(cal.month),
        "year": ranges(cal.year),
        "day_of_week": ranges(cal.day_of_week),
        "comment": cal.comment,
    }


def _deserialize_calendar(d: Mapping[str, Any]) -> ScheduleCalendarSpec:
    def ranges(key: str) -> Sequence[ScheduleRange]:
        return tuple(ScheduleRange(*r) for r in d.get(key, []))

    return ScheduleCalendarSpec(
        second=ranges("second"),
        minute=ranges("minute"),
        hour=ranges("hour"),
        day_of_month=ranges("day_of_month"),
        month=ranges("month"),
        year=ranges("year"),
        day_of_week=ranges("day_of_week"),
        comment=d.get("comment"),
    )


def _serialize_spec(spec: ScheduleSpec) -> Dict[str, Any]:
    return {
        "intervals": [
            {"every": i.every.total_seconds(), "offset": _secs(i.offset)}
            for i in spec.intervals
        ],
        "calendars": [_serialize_calendar(c) for c in spec.calendars],
        "cron_expressions": list(spec.cron_expressions),
        "skip": [_serialize_calendar(c) for c in spec.skip],
        "start_at": spec.start_at.isoformat() if spec.start_at else None,
        "end_at": spec.end_at.isoformat() if spec.end_at else None,
        "jitter": _secs(spec.jitter),
        "time_zone_name": spec.time_zone_name,
    }


def _deserialize_spec(d: Mapping[str, Any]) -> ScheduleSpec:
    return ScheduleSpec(
        calendars=[_deserialize_calendar(c) for c in d.get("calendars", [])],
        intervals=[
            ScheduleIntervalSpec(
                every=timedelta(seconds=i["every"]),
                offset=_td(i.get("offset")),
            )
            for i in d.get("intervals", [])
        ],
        cron_expressions=list(d.get("cron_expressions", [])),
        skip=[_deserialize_calendar(c) for c in d.get("skip", [])],
        start_at=_parse_dt(d.get("start_at")),
        end_at=_parse_dt(d.get("end_at")),
        jitter=_td(d.get("jitter")),
        time_zone_name=d.get("time_zone_name"),
    )


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    return datetime.fromisoformat(value) if value else None


def serialize_schedule_context(schedule: Schedule) -> Dict[str, Any]:
    """Build the DBOS ``context`` dict carried by the schedule row."""
    action = schedule.action
    if not isinstance(action, ScheduleActionStartWorkflow):
        raise TypeError(
            "temporal-dbos schedules support ScheduleActionStartWorkflow only"
        )
    return {
        "action": {
            "workflow": _resolve_type_name(action.workflow),
            "args": conversion.encode_values_sync(list(action.args)),
            "id": action.id,
            "task_queue": action.task_queue,
            "execution_timeout": _secs(action.execution_timeout),
            "run_timeout": _secs(action.run_timeout),
            "task_timeout": _secs(action.task_timeout),
            "retry_policy": (
                serialize_retry_policy(action.retry_policy)
                if action.retry_policy is not None
                else None
            ),
        },
        "spec": _serialize_spec(schedule.spec),
        "policy": {
            "overlap": int(schedule.policy.overlap),
            "catchup_window": schedule.policy.catchup_window.total_seconds(),
            "pause_on_failure": schedule.policy.pause_on_failure,
        },
        "state": {
            "note": schedule.state.note,
            "paused": schedule.state.paused,
            "limited_actions": schedule.state.limited_actions,
            "remaining_actions": schedule.state.remaining_actions,
        },
    }


def _action_from_context(ctx: Mapping[str, Any]) -> ScheduleActionStartWorkflow:
    a = ctx["action"]
    return ScheduleActionStartWorkflow(
        a["workflow"],
        args=[conversion.decode_value_sync(p) for p in a.get("args", [])],
        id=a["id"],
        task_queue=a["task_queue"],
        execution_timeout=_td(a.get("execution_timeout")),
        run_timeout=_td(a.get("run_timeout")),
        task_timeout=_td(a.get("task_timeout")),
    )


def _schedule_from_context(ctx: Mapping[str, Any]) -> Schedule:
    policy = ctx.get("policy", {})
    state = ctx.get("state", {})
    return Schedule(
        action=_action_from_context(ctx),
        spec=_deserialize_spec(ctx.get("spec", {})),
        policy=SchedulePolicy(
            overlap=ScheduleOverlapPolicy(
                policy.get("overlap", ScheduleOverlapPolicy.SKIP)
            ),
            catchup_window=timedelta(seconds=policy.get("catchup_window", 31536000.0)),
            pause_on_failure=policy.get("pause_on_failure", False),
        ),
        state=ScheduleState(
            note=state.get("note"),
            paused=state.get("paused", False),
            limited_actions=state.get("limited_actions", False),
            remaining_actions=state.get("remaining_actions", 0),
        ),
    )


def compile_spec(spec: ScheduleSpec) -> "tuple[str, Optional[str]]":
    """Compile a ``ScheduleSpec`` to a (cron, timezone_name) pair for DBOS.

    cron_expressions win, then intervals, then calendars. Extra entries beyond
    the first are dropped with a deviation (DEVIATIONS D22)."""
    tz_name = spec.time_zone_name
    if spec.cron_expressions:
        if len(spec.cron_expressions) > 1:
            _schedules.logger.debug(
                "multiple cron_expressions on a schedule; using the first "
                "(DEVIATIONS D22)"
            )
        fields, tz = _schedules.parse_cron(spec.cron_expressions[0])
        return fields, tz_name or (None if str(tz) == "UTC" else str(tz))
    if spec.intervals:
        if len(spec.intervals) > 1:
            _schedules.logger.debug(
                "multiple intervals on a schedule; using the first (DEVIATIONS D22)"
            )
        i = spec.intervals[0]
        return _schedules.interval_to_cron(i.every, i.offset), tz_name
    if spec.calendars:
        if len(spec.calendars) > 1:
            _schedules.logger.debug(
                "multiple calendars on a schedule; using the first (DEVIATIONS D22)"
            )
        c = spec.calendars[0]

        def rng(rs: Sequence[ScheduleRange]) -> Sequence["tuple[int, int, int]"]:
            return [(r.start, r.end, r.step) for r in rs]

        cron = _schedules.calendar_to_cron(
            second=rng(c.second),
            minute=rng(c.minute),
            hour=rng(c.hour),
            day_of_month=rng(c.day_of_month),
            month=rng(c.month),
            day_of_week=rng(c.day_of_week),
            year=rng(c.year),
        )
        return cron, tz_name
    raise ValueError("ScheduleSpec must specify intervals, calendars, or cron")


# ---------------------------------------------------------------------------
# Describe construction + create/recreate helpers (used by Client)
# ---------------------------------------------------------------------------


def _next_action_times(ctx: Mapping[str, Any], count: int) -> List[datetime]:
    try:
        spec = _deserialize_spec(ctx.get("spec", {}))
        cron, _tz = compile_spec(spec)
    except ValueError:
        return []
    from dbos._croniter import croniter  # type: ignore[attr-defined]

    now = datetime.now(timezone.utc)
    it = croniter(cron, now, second_at_beginning=True)
    return [it.get_next(datetime) for _ in range(count)]


def _description_from_row(row: Mapping[str, Any]) -> ScheduleDescription:
    ctx = row["context"]
    schedule = _schedule_from_context(ctx)
    schedule.state.paused = row.get("status") != "ACTIVE"
    info = ScheduleInfo(
        num_actions=0,
        num_actions_missed_catchup_window=0,
        num_actions_skipped_overlap=0,
        running_actions=[],
        recent_actions=[],
        next_action_times=_next_action_times(ctx, 10),
        created_at=_parse_dt(ctx.get("created_at")) or datetime.now(timezone.utc),
        last_updated_at=None,
    )
    return ScheduleDescription(id=row["schedule_name"], schedule=schedule, info=info)


def _list_description_from_row(row: Mapping[str, Any]) -> ScheduleListDescription:
    ctx = row["context"]
    action = ctx.get("action", {})
    state = ctx.get("state", {})
    return ScheduleListDescription(
        id=row["schedule_name"],
        schedule=ScheduleListSchedule(
            action=ScheduleListActionStartWorkflow(workflow=action.get("workflow", "")),
            spec=_deserialize_spec(ctx.get("spec", {})),
            state=ScheduleListState(
                note=state.get("note"),
                paused=row.get("status") != "ACTIVE",
            ),
        ),
        info=ScheduleListInfo(
            recent_actions=[], next_action_times=_next_action_times(ctx, 10)
        ),
    )


def require_supported_overlap(overlap: Optional[ScheduleOverlapPolicy]) -> None:
    """Reject overlap policies temporal-dbos does not implement (DEVIATIONS D22).

    SKIP, CANCEL_OTHER, TERMINATE_OTHER, and ALLOW_ALL are honored;
    BUFFER_ONE/BUFFER_ALL need durable start-after-completion queueing we don't
    do yet, so we fail loudly. ``None`` (a trigger/backfill override that defers
    to the schedule's policy) is allowed.
    """
    if overlap in (
        ScheduleOverlapPolicy.BUFFER_ONE,
        ScheduleOverlapPolicy.BUFFER_ALL,
    ):
        raise NotImplementedError(
            "temporal-dbos does not support ScheduleOverlapPolicy.BUFFER_ONE / "
            "BUFFER_ALL yet (DEVIATIONS D22); SKIP, CANCEL_OTHER, "
            "TERMINATE_OTHER, and ALLOW_ALL are supported"
        )


def require_overlap_override_supported(
    overlap: Optional[ScheduleOverlapPolicy],
) -> None:
    """Reject a per-call (trigger/backfill) overlap override we don't honor
    (DEVIATIONS D22). A per-call override can't be threaded through DBOS's
    trigger/backfill, so only ``None`` (use the schedule's configured policy)
    and ``ALLOW_ALL`` (the least-restrictive, no-op case) are accepted; any
    other value raises rather than being silently ignored."""
    if overlap is not None and overlap != ScheduleOverlapPolicy.ALLOW_ALL:
        raise NotImplementedError(
            "temporal-dbos does not honor a per-call ScheduleOverlapPolicy "
            f"override of {overlap!r} on trigger/backfill (DEVIATIONS D22); the "
            "schedule's configured overlap policy applies. Only None or "
            "ALLOW_ALL are accepted."
        )


async def create_schedule_row(
    client: "Client",
    id: str,
    schedule: Schedule,
    *,
    trigger_immediately: bool = False,
    backfill: Sequence[ScheduleBackfill] = [],
) -> None:
    """Compile and create the DBOS schedule row for a Temporal schedule."""
    if not isinstance(schedule.action, ScheduleActionStartWorkflow):
        raise TypeError(
            "temporal-dbos schedules support ScheduleActionStartWorkflow only"
        )
    require_supported_overlap(schedule.policy.overlap)
    for b in backfill:
        require_overlap_override_supported(b.overlap)
    from ._internal import ids as _ids

    _ids.validate_workflow_id(schedule.action.id)
    cron, tz_name = compile_spec(schedule.spec)
    context = serialize_schedule_context(schedule)
    # The fire dispatcher needs the compiled cron + timezone to walk prior
    # occurrences for overlap handling, and created_at to bound that walk
    # (and to back describe()'s ScheduleInfo.created_at).
    context["cron"] = cron
    context["timezone"] = tz_name
    context["created_at"] = datetime.now(timezone.utc).isoformat()
    await client._dbos_client.create_schedule_async(
        schedule_name=id,
        workflow_name=SCHEDULE_FIRE_WORKFLOW,
        schedule=cron,
        context=context,
        cron_timezone=tz_name,
        queue_name=schedule.action.task_queue,
    )
    if schedule.state.paused:
        await asyncio.to_thread(client._dbos_client.pause_schedule, id)
    if trigger_immediately:
        await asyncio.to_thread(client._dbos_client.trigger_schedule, id)
    for b in backfill:
        await asyncio.to_thread(
            client._dbos_client.backfill_schedule, id, b.start_at, b.end_at
        )


async def _replace_schedule(client: "Client", id: str, schedule: Schedule) -> None:
    await client._dbos_client.delete_schedule_async(id)
    await create_schedule_row(client, id, schedule)

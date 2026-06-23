"""Schedules: the temporalio-shaped ``Schedule`` type surface
and ``ScheduleHandle``, backed by DBOS schedule primitives.

Mirrors ``temporalio.client``'s schedule types (re-exported from
``dbosify.client``). A Temporal ``Schedule`` compiles to a single DBOS
schedule row: ``DBOS.create_schedule`` fires a generic dispatcher workflow
(``__temporal_schedule_fire``) which, at each occurrence, starts the action
workflow with a per-occurrence deterministic id (see
``_internal/dispatcher.py``). The action and the full ``ScheduleSpec`` ride in
the schedule's ``context`` so ``describe``/``list``/``update`` can reconstruct
it; ``ScheduleSpec`` itself compiles down to a cron string + timezone for DBOS.

Deviations (ARCHITECTURE schedules): interval periods that don't divide a cron
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

from ._internal import attributes as _attributes
from ._internal import conversion
from ._internal import registry as _registry
from ._internal import schedules as _schedules
from ._internal.client_interceptor import (
    BackfillScheduleInput,
    DeleteScheduleInput,
    DescribeScheduleInput,
    PauseScheduleInput,
    TriggerScheduleInput,
    UnpauseScheduleInput,
    UpdateScheduleInput,
)
from ._internal.payloads import deserialize_retry_policy, serialize_retry_policy
from .common import Priority, RetryPolicy, SearchAttributes, TypedSearchAttributes

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
    rejected at ``create_schedule`` time (ARCHITECTURE schedules)."""

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
        typed_search_attributes: TypedSearchAttributes = TypedSearchAttributes.empty,
        untyped_search_attributes: SearchAttributes = {},
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
        self.typed_search_attributes = typed_search_attributes
        self.untyped_search_attributes = untyped_search_attributes
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
        return await self._client._impl.describe_schedule(
            DescribeScheduleInput(
                id=self.id, rpc_metadata=rpc_metadata, rpc_timeout=rpc_timeout
            )
        )

    async def _describe_impl(self, input: DescribeScheduleInput) -> ScheduleDescription:
        row = await self._client._dbos_client.get_schedule_async(self.id)
        if row is None:
            raise RuntimeError(f"Schedule {self.id!r} not found")
        return await _description_from_row(row)

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
        to skip). Implemented as delete-then-recreate (ARCHITECTURE schedules)."""
        await self._client._impl.update_schedule(
            UpdateScheduleInput(
                id=self.id,
                updater=updater,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def _update_impl(self, input: UpdateScheduleInput) -> None:
        # Read the row directly rather than via describe(), so a describe_schedule
        # interceptor isn't invoked as a side effect of an update.
        row = await self._client._dbos_client.get_schedule_async(self.id)
        if row is None:
            raise RuntimeError(f"Schedule {self.id!r} not found")
        desc = await _description_from_row(row)
        outcome = input.updater(ScheduleUpdateInput(description=desc))
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
        but not persisted (ARCHITECTURE schedules)."""
        await self._client._impl.pause_schedule(
            PauseScheduleInput(
                id=self.id,
                note=note,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def _pause_impl(self, input: PauseScheduleInput) -> None:
        await asyncio.to_thread(self._client._dbos_client.pause_schedule, self.id)

    async def unpause(
        self,
        *,
        note: Optional[str] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Unpause this schedule (DBOS ``resume_schedule``). ``note`` is accepted
        but not persisted (ARCHITECTURE schedules)."""
        await self._client._impl.unpause_schedule(
            UnpauseScheduleInput(
                id=self.id,
                note=note,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def _unpause_impl(self, input: UnpauseScheduleInput) -> None:
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
        is accepted only as ``ALLOW_ALL`` (others raise — ARCHITECTURE schedules)."""
        await self._client._impl.trigger_schedule(
            TriggerScheduleInput(
                id=self.id,
                overlap=overlap,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def _trigger_impl(self, input: TriggerScheduleInput) -> None:
        require_overlap_override_supported(input.overlap)
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
        ARCHITECTURE schedules)."""
        await self._client._impl.backfill_schedule(
            BackfillScheduleInput(
                id=self.id,
                backfills=list(backfill),
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def _backfill_impl(self, input: BackfillScheduleInput) -> None:
        if not input.backfills:
            raise ValueError("At least one backfill required")
        for b in input.backfills:
            require_overlap_override_supported(b.overlap)
        for b in input.backfills:
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
        await self._client._impl.delete_schedule(
            DeleteScheduleInput(
                id=self.id, rpc_metadata=rpc_metadata, rpc_timeout=rpc_timeout
            )
        )

    async def _delete_impl(self, input: DeleteScheduleInput) -> None:
        await self._client._dbos_client.delete_schedule_async(self.id)


# Context (de)serialization — the schedule's DBOS ``context`` payload; the fire
# dispatcher reads ``context["action"]`` directly (see dispatcher.py).


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
        raise TypeError("dbosify schedules support ScheduleActionStartWorkflow only")
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
            "static_summary": action.static_summary,
            "static_details": action.static_details,
            "priority": _serialize_priority(action.priority),
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


async def encode_action_attributes(
    action: ScheduleActionStartWorkflow,
) -> Optional[Dict[str, Any]]:
    """Encode the action's memo + search attributes into the DBOS attributes
    dict applied to each workflow the schedule starts (the same namespaced
    form ``RunMeta.attributes`` carries). Typed attributes win over untyped on
    a name clash, mirroring temporalio."""
    sa = {
        **_attributes.encode_search_attributes(action.untyped_search_attributes),
        **_attributes.encode_search_attributes(action.typed_search_attributes),
    }
    out: Dict[str, Any] = {}
    if action.memo:
        out[_attributes.MEMO_KEY] = await _attributes.encode_memo(action.memo)
    if sa:
        out[_attributes.SEARCH_ATTRIBUTES_KEY] = sa
    return out or None


def _serialize_priority(priority: Any) -> Optional[Dict[str, Any]]:
    """Serialize a ``common.Priority`` to a plain dict (or None). Priority is
    inert here, but round-tripped so describe()/update() preserve it."""
    if not isinstance(priority, Priority):
        return None
    return {
        "priority_key": priority.priority_key,
        "fairness_key": priority.fairness_key,
        "fairness_weight": priority.fairness_weight,
    }


def _deserialize_priority(raw: Optional[Mapping[str, Any]]) -> Optional[Priority]:
    if raw is None:
        return None
    return Priority(
        priority_key=raw.get("priority_key"),
        fairness_key=raw.get("fairness_key"),
        fairness_weight=raw.get("fairness_weight"),
    )


async def _action_from_context(ctx: Mapping[str, Any]) -> ScheduleActionStartWorkflow:
    a = ctx["action"]
    # Fully round-trip the action so describe()/update() re-encode to the same
    # stored form (untyped search attributes come back as typed, as in temporalio).
    memo, typed_sa = await _attributes.decode_attributes(a.get("attributes"))
    serialized_retry = a["retry_policy"]
    return ScheduleActionStartWorkflow(
        a["workflow"],
        args=[conversion.decode_value_sync(p) for p in a["args"]],
        id=a["id"],
        task_queue=a["task_queue"],
        execution_timeout=_td(a["execution_timeout"]),
        run_timeout=_td(a["run_timeout"]),
        task_timeout=_td(a["task_timeout"]),
        retry_policy=(
            deserialize_retry_policy(serialized_retry)
            if serialized_retry is not None
            else None
        ),
        memo=memo or None,
        typed_search_attributes=typed_sa,
        static_summary=a["static_summary"],
        static_details=a["static_details"],
        priority=_deserialize_priority(a["priority"]),
    )


async def _schedule_from_context(ctx: Mapping[str, Any]) -> Schedule:
    policy = ctx["policy"]
    state = ctx["state"]
    return Schedule(
        action=await _action_from_context(ctx),
        spec=_deserialize_spec(ctx["spec"]),
        policy=SchedulePolicy(
            overlap=ScheduleOverlapPolicy(policy["overlap"]),
            catchup_window=timedelta(seconds=policy["catchup_window"]),
            pause_on_failure=policy["pause_on_failure"],
        ),
        state=ScheduleState(
            note=state["note"],
            paused=state["paused"],
            limited_actions=state["limited_actions"],
            remaining_actions=state["remaining_actions"],
        ),
    )


def compile_spec(spec: ScheduleSpec) -> "tuple[str, Optional[str]]":
    """Compile a ``ScheduleSpec`` to a (cron, timezone_name) pair for DBOS.

    cron_expressions win, then intervals, then calendars. Extra entries beyond
    the first are dropped with a deviation (ARCHITECTURE schedules)."""
    tz_name = spec.time_zone_name
    if spec.cron_expressions:
        if len(spec.cron_expressions) > 1:
            _schedules.logger.debug(
                "multiple cron_expressions on a schedule; using the first"
            )
        fields, tz = _schedules.parse_cron(spec.cron_expressions[0])
        return fields, tz_name or (None if str(tz) == "UTC" else str(tz))
    if spec.intervals:
        if len(spec.intervals) > 1:
            _schedules.logger.debug("multiple intervals on a schedule; using the first")
        i = spec.intervals[0]
        return _schedules.interval_to_cron(i.every, i.offset), tz_name
    if spec.calendars:
        if len(spec.calendars) > 1:
            _schedules.logger.debug("multiple calendars on a schedule; using the first")
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


async def _description_from_row(row: Mapping[str, Any]) -> ScheduleDescription:
    ctx = row["context"]
    schedule = await _schedule_from_context(ctx)
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
    """Reject overlap policies dbosify does not implement (ARCHITECTURE schedules).

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
            "dbosify does not support ScheduleOverlapPolicy.BUFFER_ONE / "
            "BUFFER_ALL; use SKIP, CANCEL_OTHER, TERMINATE_OTHER, or ALLOW_ALL"
        )


def require_overlap_override_supported(
    overlap: Optional[ScheduleOverlapPolicy],
) -> None:
    """Reject a per-call (trigger/backfill) overlap override we don't honor
    (ARCHITECTURE schedules). A per-call override can't be threaded through DBOS's
    trigger/backfill, so only ``None`` (use the schedule's configured policy)
    and ``ALLOW_ALL`` (the least-restrictive, no-op case) are accepted; any
    other value raises rather than being silently ignored."""
    if overlap is not None and overlap != ScheduleOverlapPolicy.ALLOW_ALL:
        raise NotImplementedError(
            "dbosify does not honor a per-call ScheduleOverlapPolicy "
            f"override of {overlap!r} on trigger/backfill; only None or "
            "ALLOW_ALL are accepted"
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
        raise TypeError("dbosify schedules support ScheduleActionStartWorkflow only")
    require_supported_overlap(schedule.policy.overlap)
    for b in backfill:
        require_overlap_override_supported(b.overlap)
    from ._internal import ids as _ids

    _ids.validate_workflow_id(schedule.action.id)
    cron, tz_name = compile_spec(schedule.spec)
    context = serialize_schedule_context(schedule)
    # Encode the action's memo + search attributes once at create time (memo
    # rides the async converter); the fire path applies them to each start.
    action_attributes = await encode_action_attributes(schedule.action)
    if action_attributes is not None:
        context["action"]["attributes"] = action_attributes
    # Carried in the DBOS context: cron gates overlap handling, created_at backs
    # describe(), schedule_id tags each fire so the dispatcher finds prior occurrences.
    context["cron"] = cron
    context["created_at"] = datetime.now(timezone.utc).isoformat()
    context["schedule_id"] = id
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

"""Workflow author API, mirroring ``temporalio.workflow``.

Phase 0 subset: the definition decorators (``defn``/``run``/``signal``/
``query``/``update``/``init``) and the runtime functions the interpreter
backs (``execute_activity``, ``start_activity``, ``sleep``,
``wait_condition``, deterministic time/randomness, ``info``). Signatures
mirror temporalio; parameters Phase 0 does not yet honor are accepted and
ignored with a debug log, never an error.

Workflow code runs on the deterministic virtual event loop hosted by
``_internal/interpreter.py``; every function here resolves the interpreter
from the running loop and delegates.
"""

import asyncio
import inspect
import logging
import uuid as uuid_mod
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import IntEnum
from random import Random
from typing import (
    Any,
    Callable,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    overload,
)

from ._internal import registry as _registry
from .common import RetryPolicy

__all__ = [
    "ActivityCancellationType",
    "ActivityHandle",
    "ChildWorkflowCancellationType",
    "ChildWorkflowHandle",
    "ExternalWorkflowHandle",
    "Info",
    "ParentClosePolicy",
    "cancellation_reason",
    "defn",
    "execute_activity",
    "execute_activity_method",
    "execute_child_workflow",
    "execute_local_activity",
    "execute_local_activity_method",
    "get_external_workflow_handle",
    "get_external_workflow_handle_for",
    "in_workflow",
    "info",
    "init",
    "logger",
    "now",
    "query",
    "random",
    "run",
    "signal",
    "sleep",
    "start_activity",
    "start_activity_method",
    "start_child_workflow",
    "start_local_activity",
    "start_local_activity_method",
    "time",
    "time_ns",
    "unsafe",
    "update",
    "uuid4",
    "wait_condition",
]

logger = logging.getLogger("temporal_dbos.workflow")

_F = TypeVar("_F", bound=Callable[..., Any])
_CT = TypeVar("_CT", bound=type)

_arg_unset = object()


# ---------------------------------------------------------------------------
# Definition decorators
# ---------------------------------------------------------------------------


@overload
def defn(cls: _CT) -> _CT: ...


@overload
def defn(
    *,
    name: Optional[str] = None,
    sandboxed: bool = True,
    failure_exception_types: Sequence[Type[BaseException]] = [],
) -> Callable[[_CT], _CT]: ...


def defn(
    cls: Optional[_CT] = None,
    *,
    name: Optional[str] = None,
    sandboxed: bool = True,
    failure_exception_types: Sequence[Type[BaseException]] = [],
) -> Union[_CT, Callable[[_CT], _CT]]:
    """Decorator for workflow classes. ``sandboxed`` is accepted and ignored
    (temporal-dbos runs no sandbox — see the README deviations table).
    """

    def decorator(cls: _CT) -> _CT:
        defn = _registry.build_workflow_definition(
            cls, name=name, failure_exception_types=failure_exception_types
        )
        setattr(cls, _registry.WORKFLOW_DEFN_ATTR, defn)
        return cls

    if cls is not None:
        return decorator(cls)
    return decorator


def run(fn: _F) -> _F:
    """Decorator for the workflow run method. Must be an async function."""
    if not inspect.iscoroutinefunction(fn):
        raise ValueError("Workflow run method must be an async function")
    setattr(fn, _registry.RUN_ATTR, True)
    return fn


@overload
def signal(fn: _F) -> _F: ...


@overload
def signal(*, name: str) -> Callable[[_F], _F]: ...


def signal(
    fn: Optional[_F] = None, *, name: Optional[str] = None
) -> Union[_F, Callable[[_F], _F]]:
    """Decorator for a workflow signal handler method."""

    def decorator(fn: _F) -> _F:
        setattr(fn, _registry.SIGNAL_ATTR, name if name is not None else fn.__name__)
        return fn

    if fn is not None:
        return decorator(fn)
    return decorator


@overload
def query(fn: _F) -> _F: ...


@overload
def query(*, name: str) -> Callable[[_F], _F]: ...


def query(
    fn: Optional[_F] = None, *, name: Optional[str] = None
) -> Union[_F, Callable[[_F], _F]]:
    """Decorator for a workflow query handler method. Must be synchronous in
    Phase 0.
    """

    def decorator(fn: _F) -> _F:
        if inspect.iscoroutinefunction(fn):
            raise ValueError("Query handlers must be synchronous in temporal-dbos v0")
        setattr(fn, _registry.QUERY_ATTR, name if name is not None else fn.__name__)
        return fn

    if fn is not None:
        return decorator(fn)
    return decorator


class _UpdateMethod:
    """Stand-in for temporalio's update method wrapper: carries the handler,
    its name, and an optional validator registered via ``@handler.validator``.
    """

    def __init__(self, fn: Callable[..., Any], name: Optional[str]) -> None:
        self.fn = fn
        self.name = name if name is not None else fn.__name__
        self.validator_fn: Optional[Callable[..., Any]] = None

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.fn(*args, **kwargs)

    def validator(self, vfn: Callable[..., Any]) -> Callable[..., Any]:
        """Decorator for the update's validator. Must be synchronous."""
        if inspect.iscoroutinefunction(vfn):
            raise ValueError("Update validators must be synchronous")
        self.validator_fn = vfn
        return vfn


class ExternalWorkflowHandle:
    """Handle for interacting with an *external* workflow from within a
    workflow: signal and (cooperatively) cancel, both as checkpointed sends
    targeting the id's current run.
    """

    def __init__(
        self, runtime: "_Runtime", workflow_id: str, run_id: Optional[str] = None
    ) -> None:
        self._runtime = runtime
        self._id = workflow_id
        self._run_id = run_id

    @property
    def id(self) -> str:
        """ID of the external workflow."""
        return self._id

    @property
    def run_id(self) -> Optional[str]:
        """Run ID of the external workflow, if bound."""
        return self._run_id

    async def signal(
        self, signal: Any, arg: Any = _arg_unset, *, args: Sequence[Any] = []
    ) -> None:
        """Send a signal to the external workflow."""
        from ._internal import inbox as _inbox

        name = (
            signal
            if isinstance(signal, str)
            else getattr(signal, _registry.SIGNAL_ATTR)
        )
        await self._runtime.runtime_send_to_workflow(
            self._run_id or self._id,
            _inbox.signal_envelope(str(name), _resolve_args(arg, args)),
            resolve_chain=self._run_id is None,
        )

    async def cancel(self, *, reason: str = "") -> None:
        """Request cooperative cancellation of the external workflow."""
        from ._internal import inbox as _inbox

        await self._runtime.runtime_send_to_workflow(
            self._run_id or self._id,
            _inbox.cancel_envelope(reason),
            resolve_chain=self._run_id is None,
        )


@overload
def update(fn: Callable[..., Any]) -> _UpdateMethod: ...


@overload
def update(*, name: str) -> Callable[[Callable[..., Any]], _UpdateMethod]: ...


def update(
    fn: Optional[Callable[..., Any]] = None, *, name: Optional[str] = None
) -> Union[_UpdateMethod, Callable[[Callable[..., Any]], _UpdateMethod]]:
    """Decorator for a workflow update handler method. Attach a validator
    with ``@my_update.validator``.
    """

    def decorator(fn: Callable[..., Any]) -> _UpdateMethod:
        return _UpdateMethod(fn, name)

    if fn is not None:
        return decorator(fn)
    return decorator


def init(init_fn: _F) -> _F:
    """Decorator for ``__init__`` to receive the same arguments as run."""
    if init_fn.__name__ != "__init__":
        raise ValueError("@workflow.init may only be used on __init__")
    setattr(init_fn, _registry.INIT_ATTR, True)
    return init_fn


# ---------------------------------------------------------------------------
# Runtime context
# ---------------------------------------------------------------------------


class ActivityCancellationType(IntEnum):
    """How a workflow cancels an activity, mirroring
    ``temporalio.workflow.ActivityCancellationType``. Phase 2 honors
    TRY_CANCEL and ABANDON; WAIT_CANCELLATION_COMPLETED is approximated as
    TRY_CANCEL until Phase 3's activity-side cancellation observation.
    """

    TRY_CANCEL = 0
    WAIT_CANCELLATION_COMPLETED = 1
    ABANDON = 2


class ParentClosePolicy(IntEnum):
    """What to do with running children when the parent closes, mirroring
    ``temporalio.workflow.ParentClosePolicy``.
    """

    UNSPECIFIED = 0
    TERMINATE = 1
    ABANDON = 2
    REQUEST_CANCEL = 3


class ChildWorkflowCancellationType(IntEnum):
    """How a workflow cancels a child workflow, mirroring
    ``temporalio.workflow.ChildWorkflowCancellationType``. Cancellation is
    delivered as the child's cooperative-cancel envelope; the WAIT variants
    are approximated as TRY_CANCEL while the awaiter is gone (same
    constraint as activities).
    """

    ABANDON = 0
    TRY_CANCEL = 1
    WAIT_CANCELLATION_COMPLETED = 2
    WAIT_CANCELLATION_REQUESTED = 3


@dataclass(frozen=True)
class Info:
    """Information about the running workflow (Phase 0 subset of
    temporalio's ``workflow.Info``).
    """

    attempt: int
    namespace: str
    run_id: str
    start_time: datetime
    task_queue: str
    workflow_id: str
    workflow_type: str


class _Runtime:
    """Interface the interpreter implements to back this module's functions.

    Defined here (not in _internal) so _internal modules can import it
    without cycles.
    """

    def runtime_info(self) -> Info:
        raise NotImplementedError

    def runtime_now(self) -> float:
        raise NotImplementedError

    def runtime_random(self) -> Random:
        raise NotImplementedError

    def runtime_is_replaying(self) -> bool:
        raise NotImplementedError

    def runtime_cancellation_reason(self) -> Optional[str]:
        raise NotImplementedError

    def runtime_start_activity(
        self,
        activity_name: str,
        args: Sequence[Any],
        *,
        schedule_to_close_timeout: Optional[timedelta],
        start_to_close_timeout: Optional[timedelta],
        retry_policy: Optional[RetryPolicy],
        activity_id: Optional[str],
        cancellation_type: int = 0,
    ) -> "ActivityHandle":
        raise NotImplementedError

    async def runtime_wait_condition(
        self, fn: Callable[[], bool], *, timeout: Optional[float]
    ) -> None:
        raise NotImplementedError

    async def runtime_start_child_workflow(
        self,
        type_name: str,
        args: Sequence[Any],
        *,
        child_id: Optional[str],
        task_queue: Optional[str],
        parent_close_policy: int,
        cancellation_type: int,
    ) -> "ChildWorkflowHandle":
        raise NotImplementedError

    async def runtime_send_to_workflow(
        self, workflow_id: str, envelope: Any, *, resolve_chain: bool = False
    ) -> None:
        raise NotImplementedError


class ActivityHandle:
    """Handle to a started activity: awaitable for its result.

    Cancellation reaches it implicitly (workflow cancel, ``wait_for``
    timeouts); an explicit ``cancel()`` lands with Phase 3's activity-side
    observation.
    """

    def __init__(self, future: "asyncio.Future[Any]") -> None:
        self._future = future

    def __await__(self) -> Any:
        return self._future.__await__()

    def done(self) -> bool:
        return self._future.done()

    def result(self) -> Any:
        return self._future.result()


class ChildWorkflowHandle:
    """Handle to a started child workflow: awaitable for its result, plus
    ``signal`` (checkpointed send from the parent's perspective).
    """

    def __init__(
        self, runtime: "_Runtime", workflow_id: str, future: "asyncio.Future[Any]"
    ) -> None:
        self._runtime = runtime
        self._id = workflow_id
        self._future = future

    @property
    def id(self) -> str:
        """ID of the child workflow."""
        return self._id

    @property
    def first_execution_run_id(self) -> Optional[str]:
        """Run ID of the child's first run (its DBOS workflow id)."""
        return self._id

    def __await__(self) -> Any:
        return self._future.__await__()

    def done(self) -> bool:
        return self._future.done()

    async def signal(
        self, signal: Any, arg: Any = _arg_unset, *, args: Sequence[Any] = []
    ) -> None:
        """Send a signal to the child workflow."""
        from ._internal import inbox as _inbox

        name = (
            signal
            if isinstance(signal, str)
            else getattr(signal, _registry.SIGNAL_ATTR)
        )
        await self._runtime.runtime_send_to_workflow(
            self._id, _inbox.signal_envelope(str(name), _resolve_args(arg, args))
        )


def _runtime() -> _Runtime:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    runtime = getattr(loop, "tdb_runtime", None)
    if runtime is None:
        raise RuntimeError("Not in workflow event loop")
    assert isinstance(runtime, _Runtime)
    return runtime


def in_workflow() -> bool:
    """Whether the current code is inside a workflow."""
    try:
        _runtime()
        return True
    except RuntimeError:
        return False


# ---------------------------------------------------------------------------
# Runtime functions
# ---------------------------------------------------------------------------


def info() -> Info:
    """Current workflow's info."""
    return _runtime().runtime_info()


def cancellation_reason() -> Optional[str]:
    """The reason for the workflow's cancellation request, if any."""
    return _runtime().runtime_cancellation_reason()


def now() -> datetime:
    """Current workflow time: deterministic, advances only on events."""
    return datetime.fromtimestamp(time(), tz=None)


def time() -> float:
    """Current workflow time as seconds since the epoch."""
    return _runtime().runtime_now()


def time_ns() -> int:
    """Current workflow time as nanoseconds since the epoch."""
    return int(_runtime().runtime_now() * 1e9)


def random() -> Random:
    """Deterministically-seeded random instance for this workflow."""
    return _runtime().runtime_random()


def uuid4() -> uuid_mod.UUID:
    """Deterministic UUID v4 derived from the workflow's random seed."""
    return uuid_mod.UUID(bytes=random().getrandbits(128).to_bytes(16, "big"), version=4)


async def sleep(
    duration: Union[float, timedelta], *, summary: Optional[str] = None
) -> None:
    """Sleep for the given duration on the deterministic loop (a durable
    timer). ``summary`` is accepted and ignored in Phase 0.
    """
    seconds = duration.total_seconds() if isinstance(duration, timedelta) else duration
    await asyncio.sleep(seconds)


async def wait_condition(
    fn: Callable[[], bool],
    *,
    timeout: Optional[Union[float, timedelta]] = None,
    timeout_summary: Optional[str] = None,
) -> None:
    """Wait on a callback to become true, re-evaluated on each new event.
    Raises ``asyncio.TimeoutError`` on timeout (which, uncaught, fails the
    workflow — Temporal semantics).
    """
    seconds = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
    await _runtime().runtime_wait_condition(fn, timeout=seconds)


def _resolve_activity_name(activity: Any) -> str:
    if isinstance(activity, str):
        return activity
    return _registry.activity_definition_of(activity).name


def _resolve_args(arg: Any, args: Sequence[Any]) -> List[Any]:
    if arg is not _arg_unset:
        if args:
            raise ValueError("Cannot have both arg and args")
        return [arg]
    return list(args)


def start_activity(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    result_type: Optional[type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[Any] = None,
    summary: Optional[str] = None,
    priority: Optional[Any] = None,
) -> ActivityHandle:
    """Start an activity and return its handle.

    Phase 1 honors arg/args, ``start_to_close_timeout``,
    ``schedule_to_close_timeout``, ``retry_policy``, and ``activity_id``;
    the remaining parameters are accepted and ignored (debug-logged).
    ``result_type`` is a no-op: payloads round-trip through the DBOS
    serializer, so no type hint is needed to reconstruct them.
    """
    if not start_to_close_timeout and not schedule_to_close_timeout:
        raise ValueError(
            "Activity must have start_to_close_timeout or schedule_to_close_timeout"
        )
    ignored = {
        "task_queue": task_queue,
        "schedule_to_start_timeout": schedule_to_start_timeout,
        "heartbeat_timeout": heartbeat_timeout,
        "versioning_intent": versioning_intent,
        "summary": summary,
        "priority": priority,
    }
    for key, value in ignored.items():
        if value is not None:
            logger.debug("start_activity: ignoring unsupported parameter %r", key)
    return _runtime().runtime_start_activity(
        _resolve_activity_name(activity),
        _resolve_args(arg, args),
        schedule_to_close_timeout=schedule_to_close_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        activity_id=activity_id,
        cancellation_type=int(
            cancellation_type
            if cancellation_type is not None
            else ActivityCancellationType.TRY_CANCEL
        ),
    )


async def execute_activity(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    result_type: Optional[type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[Any] = None,
    summary: Optional[str] = None,
    priority: Optional[Any] = None,
) -> Any:
    """Start an activity and wait for completion. See ``start_activity``."""
    return await start_activity(
        activity,
        arg,
        args=args,
        task_queue=task_queue,
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


def _resolve_workflow_type(workflow: Any) -> str:
    """Resolve a workflow reference: the class, its run method, or a name."""
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


async def start_child_workflow(
    workflow: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    id: Optional[str] = None,
    task_queue: Optional[str] = None,
    result_type: Optional[type] = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: Optional[timedelta] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    id_reuse_policy: Optional[Any] = None,
    retry_policy: Optional[RetryPolicy] = None,
    cron_schedule: str = "",
    memo: Optional[Any] = None,
    search_attributes: Optional[Any] = None,
    versioning_intent: Optional[Any] = None,
    static_summary: Optional[str] = None,
    static_details: Optional[str] = None,
    priority: Optional[Any] = None,
) -> ChildWorkflowHandle:
    """Start a child workflow; returns its handle once the start is durable
    (Temporal semantics: resolves on start, not completion).

    Phase 2 honors arg/args, id (default: ``{parent_id}_{seq}`` — README
    deviation #5), task_queue, parent_close_policy, and cancellation_type;
    the remaining parameters are accepted and ignored (debug-logged).
    """
    for key, value in {
        "result_type": result_type,
        "execution_timeout": execution_timeout,
        "run_timeout": run_timeout,
        "task_timeout": task_timeout,
        "id_reuse_policy": id_reuse_policy,
        "retry_policy": retry_policy,
        "cron_schedule": cron_schedule or None,
        "memo": memo,
        "search_attributes": search_attributes,
        "versioning_intent": versioning_intent,
        "static_summary": static_summary,
        "static_details": static_details,
        "priority": priority,
    }.items():
        if value is not None:
            logger.debug("start_child_workflow: ignoring unsupported option %r", key)
    return await _runtime().runtime_start_child_workflow(
        _resolve_workflow_type(workflow),
        _resolve_args(arg, args),
        child_id=id,
        task_queue=task_queue,
        parent_close_policy=int(parent_close_policy),
        cancellation_type=int(cancellation_type),
    )


def get_external_workflow_handle(
    workflow_id: str, *, run_id: Optional[str] = None
) -> ExternalWorkflowHandle:
    """Get a handle to an external workflow for signalling/cancelling. With
    no ``run_id``, operations target the id's current run.
    """
    return ExternalWorkflowHandle(_runtime(), workflow_id, run_id)


def get_external_workflow_handle_for(
    workflow: Any, workflow_id: str, *, run_id: Optional[str] = None
) -> ExternalWorkflowHandle:
    """Typed variant of :py:func:`get_external_workflow_handle`."""
    return get_external_workflow_handle(workflow_id, run_id=run_id)


async def execute_child_workflow(
    workflow: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    id: Optional[str] = None,
    task_queue: Optional[str] = None,
    result_type: Optional[type] = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: Optional[timedelta] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    id_reuse_policy: Optional[Any] = None,
    retry_policy: Optional[RetryPolicy] = None,
    cron_schedule: str = "",
    memo: Optional[Any] = None,
    search_attributes: Optional[Any] = None,
    versioning_intent: Optional[Any] = None,
    static_summary: Optional[str] = None,
    static_details: Optional[str] = None,
    priority: Optional[Any] = None,
) -> Any:
    """Start a child workflow and wait for its result. See
    ``start_child_workflow``.
    """
    handle = await start_child_workflow(
        workflow,
        arg,
        args=args,
        id=id,
        task_queue=task_queue,
        result_type=result_type,
        cancellation_type=cancellation_type,
        parent_close_policy=parent_close_policy,
        execution_timeout=execution_timeout,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        id_reuse_policy=id_reuse_policy,
        retry_policy=retry_policy,
        cron_schedule=cron_schedule,
        memo=memo,
        search_attributes=search_attributes,
        versioning_intent=versioning_intent,
        static_summary=static_summary,
        static_details=static_details,
        priority=priority,
    )
    return await handle


# Method variants: identical resolution/execution (the worker registered the
# bound method under the same name). Mirroring temporalio, these lack
# ``result_type`` — the return type is inferred from the method.
def start_activity_method(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[Any] = None,
    summary: Optional[str] = None,
    priority: Optional[Any] = None,
) -> ActivityHandle:
    """Start an activity from a method reference. See ``start_activity``."""
    return start_activity(
        activity,
        arg,
        args=args,
        task_queue=task_queue,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


async def execute_activity_method(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[Any] = None,
    summary: Optional[str] = None,
    priority: Optional[Any] = None,
) -> Any:
    """Run an activity from a method reference. See ``execute_activity``."""
    return await start_activity_method(
        activity,
        arg,
        args=args,
        task_queue=task_queue,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


def start_local_activity(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: Optional[type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    summary: Optional[str] = None,
) -> ActivityHandle:
    """Start a local activity. In temporal-dbos, in-process step execution
    *is* the local path (DESIGN §6.1.2), so this shares machinery with
    ``start_activity``.
    """
    if not start_to_close_timeout and not schedule_to_close_timeout:
        raise ValueError(
            "Activity must have start_to_close_timeout or schedule_to_close_timeout"
        )
    for key, value in {
        "schedule_to_start_timeout": schedule_to_start_timeout,
        "local_retry_threshold": local_retry_threshold,
        "summary": summary,
    }.items():
        if value is not None:
            logger.debug("start_local_activity: ignoring unsupported parameter %r", key)
    return _runtime().runtime_start_activity(
        _resolve_activity_name(activity),
        _resolve_args(arg, args),
        schedule_to_close_timeout=schedule_to_close_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        activity_id=activity_id,
        cancellation_type=int(
            cancellation_type
            if cancellation_type is not None
            else ActivityCancellationType.TRY_CANCEL
        ),
    )


async def execute_local_activity(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: Optional[type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    summary: Optional[str] = None,
) -> Any:
    """Start a local activity and wait for completion."""
    return await start_local_activity(
        activity,
        arg,
        args=args,
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        summary=summary,
    )


def start_local_activity_method(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    summary: Optional[str] = None,
) -> ActivityHandle:
    """Start a local activity from a method reference (no ``result_type``,
    mirroring temporalio: the return type is inferred from the method)."""
    return start_local_activity(
        activity,
        arg,
        args=args,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        summary=summary,
    )


async def execute_local_activity_method(
    activity: Any,
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: Optional[Any] = None,
    activity_id: Optional[str] = None,
    summary: Optional[str] = None,
) -> Any:
    """Run a local activity from a method reference."""
    return await start_local_activity_method(
        activity,
        arg,
        args=args,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        summary=summary,
    )


class unsafe:
    """Namespace for unsafe workflow calls, mirroring
    ``temporalio.workflow.unsafe``.
    """

    @staticmethod
    def is_replaying() -> bool:
        """Whether the workflow is replaying its checkpointed prefix."""
        return _runtime().runtime_is_replaying()

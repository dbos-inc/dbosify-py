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


def init(fn: _F) -> _F:
    """Decorator for ``__init__`` to receive the same arguments as run."""
    if fn.__name__ != "__init__":
        raise ValueError("@workflow.init may only be used on __init__")
    setattr(fn, _registry.INIT_ATTR, True)
    return fn


# ---------------------------------------------------------------------------
# Runtime context
# ---------------------------------------------------------------------------


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

    def runtime_start_activity(
        self,
        activity_name: str,
        args: Sequence[Any],
        *,
        schedule_to_close_timeout: Optional[timedelta],
        start_to_close_timeout: Optional[timedelta],
        retry_policy: Optional[RetryPolicy],
        activity_id: Optional[str],
    ) -> "ActivityHandle":
        raise NotImplementedError

    async def runtime_wait_condition(
        self, fn: Callable[[], bool], *, timeout: Optional[float]
    ) -> None:
        raise NotImplementedError


class ActivityHandle:
    """Handle to a started activity: awaitable for its result.

    Phase 0: result-awaiting only; ``cancel()`` lands with the Phase 2
    cancellation matrix.
    """

    def __init__(self, future: "asyncio.Future[Any]") -> None:
        self._future = future

    def __await__(self) -> Any:
        return self._future.__await__()

    def done(self) -> bool:
        return self._future.done()

    def result(self) -> Any:
        return self._future.result()


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

    Phase 0 honors arg/args, ``start_to_close_timeout``,
    ``schedule_to_close_timeout``, ``retry_policy``, and ``activity_id``;
    the remaining parameters are accepted and ignored (debug-logged).
    """
    if not start_to_close_timeout and not schedule_to_close_timeout:
        raise ValueError(
            "Activity must have start_to_close_timeout or schedule_to_close_timeout"
        )
    ignored = {
        "task_queue": task_queue,
        "schedule_to_start_timeout": schedule_to_start_timeout,
        "heartbeat_timeout": heartbeat_timeout,
        "cancellation_type": cancellation_type,
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
    )


async def execute_activity(
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
    """Start an activity and wait for completion. See ``start_activity``."""
    return await start_activity(
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


class unsafe:
    """Namespace for unsafe workflow calls, mirroring
    ``temporalio.workflow.unsafe``.
    """

    @staticmethod
    def is_replaying() -> bool:
        """Whether the workflow is replaying its checkpointed prefix."""
        return _runtime().runtime_is_replaying()

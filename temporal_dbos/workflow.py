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
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import IntEnum
from random import Random
from typing import (
    Any,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
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
    "all_handlers_finished",
    "as_completed",
    "cancellation_reason",
    "continue_as_new",
    "ContinueAsNewError",
    "defn",
    "execute_activity",
    "execute_activity_method",
    "execute_child_workflow",
    "execute_local_activity",
    "execute_local_activity_method",
    "get_external_workflow_handle",
    "get_external_workflow_handle_for",
    "HandlerUnfinishedPolicy",
    "in_workflow",
    "info",
    "init",
    "logger",
    "LoggerAdapter",
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
    "UnfinishedSignalHandlersWarning",
    "UnfinishedUpdateHandlersWarning",
    "update",
    "uuid4",
    "wait",
    "wait_condition",
]


def _maybe_runtime() -> Optional["_Runtime"]:
    try:
        loop: Optional[asyncio.AbstractEventLoop] = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    runtime = getattr(loop, "tdb_runtime", None)
    return runtime if isinstance(runtime, _Runtime) else None


class LoggerAdapter(logging.LoggerAdapter):  # type: ignore[type-arg]
    """Adapter that adds workflow details to each message and suppresses
    output during replay (mirroring ``temporalio.workflow.LoggerAdapter``;
    without suppression, recovery would re-emit every log line the first
    execution already produced).

    Attributes:
        workflow_info_on_message: Append workflow info to each message.
            Default True.
        log_during_replay: Emit logs while replaying. Default False.
    """

    def __init__(
        self, logger: logging.Logger, extra: Optional[Mapping[str, Any]]
    ) -> None:
        super().__init__(logger, extra or {})
        self.workflow_info_on_message = True
        self.log_during_replay = False

    def process(
        self, msg: Any, kwargs: "MutableMapping[str, Any]"
    ) -> "tuple[Any, MutableMapping[str, Any]]":
        if self.workflow_info_on_message:
            runtime = _maybe_runtime()
            if runtime is not None:
                workflow_info = runtime.runtime_info()
                msg_extra = {
                    "attempt": workflow_info.attempt,
                    "namespace": workflow_info.namespace,
                    "run_id": workflow_info.run_id,
                    "task_queue": workflow_info.task_queue,
                    "workflow_id": workflow_info.workflow_id,
                    "workflow_type": workflow_info.workflow_type,
                }
                msg = f"{msg} ({msg_extra})"
        return msg, kwargs

    def isEnabledFor(self, level: int) -> bool:
        if not self.log_during_replay:
            runtime = _maybe_runtime()
            if runtime is not None and runtime.runtime_is_replaying():
                return False
        return super().isEnabledFor(level)

    @property
    def base_logger(self) -> logging.Logger:
        """Underlying logger usable for actions such as adding handlers."""
        assert isinstance(self.logger, logging.Logger)
        return self.logger


logger = LoggerAdapter(logging.getLogger("temporal_dbos.workflow"), None)

_F = TypeVar("_F", bound=Callable[..., Any])
_CT = TypeVar("_CT", bound=type)

_arg_unset = object()


class HandlerUnfinishedPolicy(IntEnum):
    """What to do when a workflow finishes while a signal/update handler is
    still running, mirroring ``temporalio.workflow.HandlerUnfinishedPolicy``.
    Either way the handler is abandoned (cancelled with the execution); the
    policy controls whether that emits a warning.
    """

    WARN_AND_ABANDON = 1
    ABANDON = 2


class UnfinishedUpdateHandlersWarning(RuntimeWarning):
    """The workflow exited before all update handlers completed."""


class UnfinishedSignalHandlersWarning(RuntimeWarning):
    """The workflow exited before all signal handlers completed."""


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
def signal(
    *,
    name: Optional[str] = None,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
) -> Callable[[_F], _F]: ...


def signal(
    fn: Optional[_F] = None,
    *,
    name: Optional[str] = None,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
) -> Union[_F, Callable[[_F], _F]]:
    """Decorator for a workflow signal handler method."""

    def decorator(fn: _F) -> _F:
        setattr(fn, _registry.SIGNAL_ATTR, name if name is not None else fn.__name__)
        setattr(fn, _registry.SIGNAL_POLICY_ATTR, int(unfinished_policy))
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

    def __init__(
        self,
        fn: Callable[..., Any],
        name: Optional[str],
        unfinished_policy: "HandlerUnfinishedPolicy" = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    ) -> None:
        self.fn = fn
        self.name = name if name is not None else fn.__name__
        self.unfinished_policy = unfinished_policy
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
def update(
    *,
    name: Optional[str] = None,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
) -> Callable[[Callable[..., Any]], _UpdateMethod]: ...


def update(
    fn: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
) -> Union[_UpdateMethod, Callable[[Callable[..., Any]], _UpdateMethod]]:
    """Decorator for a workflow update handler method. Attach a validator
    with ``@my_update.validator``.
    """

    def decorator(fn: Callable[..., Any]) -> _UpdateMethod:
        return _UpdateMethod(fn, name, unfinished_policy)

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
    ``temporalio.workflow.ActivityCancellationType``. All three are honored:
    cancellation is delivered into the running attempt (observed at its next
    ``activity.heartbeat()``), and WAIT_CANCELLATION_COMPLETED resolves an
    explicit ``handle.cancel()`` only on the activity's confirmation.
    (During a workflow-cancellation unwind the awaiting coroutine is already
    cancelled, so WAIT behaves like TRY_CANCEL there — the request is still
    delivered.)
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
    # The previous run of this chain when this run was created by a
    # continuation (continue-as-new; later also retries/cron), else None.
    continued_run_id: Optional[str] = None
    namespace: str = "default"
    run_id: str = ""
    start_time: datetime = datetime.fromtimestamp(0)
    task_queue: str = ""
    workflow_id: str = ""
    workflow_type: str = ""

    def get_current_history_length(self) -> int:
        """Approximated as the run's checkpoint cursor (claimed DBOS
        function ids) — the analog of history events here."""
        return _runtime().runtime_history_length()

    def get_current_history_size(self) -> int:
        """History byte size is not tracked; always 0. Use
        :py:meth:`is_continue_as_new_suggested` (threshold on checkpoint
        count) for continue-as-new decisions."""
        return 0

    def is_continue_as_new_suggested(self) -> bool:
        """Whether this run's checkpoint count has passed the
        continue-as-new suggestion threshold
        (``TEMPORAL_DBOS_CAN_SUGGESTION_THRESHOLD``, default 10000)."""
        return _runtime().runtime_can_suggested()


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

    def runtime_history_length(self) -> int:
        raise NotImplementedError

    def runtime_can_suggested(self) -> bool:
        raise NotImplementedError

    def runtime_cancellation_reason(self) -> Optional[str]:
        raise NotImplementedError

    def runtime_all_handlers_finished(self) -> bool:
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
        heartbeat_timeout: Optional[timedelta] = None,
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
    """Handle to a started activity: awaitable for its result. Cancellation
    reaches it implicitly (workflow cancel, ``wait_for`` timeouts) or via
    :py:meth:`cancel`, honoring the activity's ``cancellation_type``.
    """

    def __init__(
        self,
        future: "asyncio.Future[Any]",
        on_cancel: Optional[Callable[[], None]] = None,
    ) -> None:
        self._future = future
        self._on_cancel = on_cancel

    def __await__(self) -> Any:
        return self._future.__await__()

    def done(self) -> bool:
        return self._future.done()

    def result(self) -> Any:
        return self._future.result()

    def cancel(self, msg: Optional[Any] = None) -> bool:
        """Request cancellation of the activity. With
        WAIT_CANCELLATION_COMPLETED the await resolves only once the
        activity has observed the request and unwound; the default
        TRY_CANCEL resolves immediately.
        """
        if self._on_cancel is not None:
            self._on_cancel()
            return True
        return self._future.cancel(msg)


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


def all_handlers_finished() -> bool:
    """Whether all in-progress signal/update handlers have finished. Used in
    wait_condition predicates to avoid returning while handlers still run.
    """
    return _runtime().runtime_all_handlers_finished()


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

    Honors arg/args, ``start_to_close_timeout``, ``schedule_to_close_timeout``,
    ``heartbeat_timeout`` (a non-heartbeating attempt fails with
    ``TimeoutType.HEARTBEAT`` and retries), ``retry_policy``,
    ``cancellation_type``, and ``activity_id``; the remaining parameters are
    accepted and ignored (debug-logged).
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
        heartbeat_timeout=heartbeat_timeout,
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


def as_completed(
    fs: "Iterable[Awaitable[Any]]", *, timeout: Optional[float] = None
) -> "Iterator[Awaitable[Any]]":
    """Return an iterator whose values are coroutines.

    This is a deterministic version of :py:func:`asyncio.as_completed` (the
    stdlib one iterates sets, whose order varies between processes). Adapted
    from temporalio's, itself adapted from CPython (both MIT).
    """
    if asyncio.isfuture(fs) or asyncio.iscoroutine(fs):
        raise TypeError(f"expect an iterable of futures, not {type(fs).__name__}")

    done: "asyncio.Queue[Optional[asyncio.Future[Any]]]" = asyncio.Queue()

    loop = asyncio.get_event_loop()
    todo: "List[asyncio.Future[Any]]" = [
        asyncio.ensure_future(f, loop=loop) for f in list(fs)
    ]
    timeout_handle = None

    def _on_timeout() -> None:
        for f in todo:
            f.remove_done_callback(_on_completion)
            done.put_nowait(None)  # Queue a dummy value for _wait_for_one().
        todo.clear()  # Can't do todo.remove(f) in the loop.

    def _on_completion(f: "asyncio.Future[Any]") -> None:
        if not todo:
            return  # _on_timeout() was here first.
        todo.remove(f)
        done.put_nowait(f)
        if not todo and timeout_handle is not None:
            timeout_handle.cancel()

    async def _wait_for_one() -> Any:
        f = await done.get()
        if f is None:
            # Dummy value from _on_timeout().
            raise asyncio.TimeoutError
        return f.result()  # May raise f.exception().

    for f in todo:
        f.add_done_callback(_on_completion)
    if todo and timeout is not None:
        timeout_handle = loop.call_later(timeout, _on_timeout)
    for _ in range(len(todo)):
        yield _wait_for_one()


async def wait(
    fs: "Iterable[Any]",
    *,
    timeout: Optional[float] = None,
    return_when: str = asyncio.ALL_COMPLETED,
) -> "Tuple[Any, Any]":
    """Wait for the Futures or Tasks given by fs to complete.

    This is a deterministic version of :py:func:`asyncio.wait`: done and
    pending are *lists in input order*, not sets (whose iteration order
    varies between processes — replay poison). Adapted from temporalio's,
    itself adapted from CPython (both MIT).
    """
    if asyncio.isfuture(fs) or asyncio.iscoroutine(fs):
        raise TypeError(f"Expect an iterable of Tasks/Futures, not {type(fs).__name__}")
    if not fs:
        raise ValueError("Sequence of Tasks/Futures must not be empty.")
    if return_when not in (
        asyncio.FIRST_COMPLETED,
        asyncio.FIRST_EXCEPTION,
        asyncio.ALL_COMPLETED,
    ):
        raise ValueError(f"Invalid return_when value: {return_when}")

    fs_list = list(fs)

    if any(asyncio.iscoroutine(f) for f in fs_list):
        raise TypeError("Passing coroutines is forbidden, use tasks explicitly.")

    loop = asyncio.get_running_loop()
    waiter: "asyncio.Future[None]" = loop.create_future()
    timeout_handle = None
    if timeout is not None:
        timeout_handle = loop.call_later(timeout, _release_waiter, waiter)
    counter = len(fs_list)

    def _on_completion(f: "asyncio.Future[Any]") -> None:
        nonlocal counter
        counter -= 1
        if (
            counter <= 0
            or return_when == asyncio.FIRST_COMPLETED
            or return_when == asyncio.FIRST_EXCEPTION
            and (not f.cancelled() and f.exception() is not None)
        ):
            if timeout_handle is not None:
                timeout_handle.cancel()
            if not waiter.done():
                waiter.set_result(None)

    for f in fs_list:
        f.add_done_callback(_on_completion)

    try:
        await waiter
    finally:
        if timeout_handle is not None:
            timeout_handle.cancel()
        for f in fs_list:
            f.remove_done_callback(_on_completion)

    done, pending = [], []
    for f in fs_list:
        if f.done():
            done.append(f)
        else:
            pending.append(f)
    return done, pending


def _release_waiter(waiter: "asyncio.Future[Any]", *_args: Any) -> None:
    if not waiter.done():
        waiter.set_result(None)


class ContinueAsNewError(BaseException):
    """Thrown by :py:func:`continue_as_new`; must escape the run method
    uncaught (mirrors temporalio: a ``BaseException`` so bare ``except
    Exception`` blocks don't swallow it)."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        self._tdb_args: Sequence[Any] = ()
        self._tdb_workflow: Optional[str] = None
        self._tdb_task_queue: Optional[str] = None


def continue_as_new(
    arg: Any = _arg_unset,
    *,
    args: Sequence[Any] = [],
    workflow: Any = None,
    task_queue: Optional[str] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    retry_policy: Optional[RetryPolicy] = None,
    memo: Optional[Any] = None,
    search_attributes: Optional[Any] = None,
    versioning_intent: Optional[Any] = None,
    initial_versioning_behavior: Optional[Any] = None,
) -> "NoReturn":
    """Stop the current run and continue the chain as a new run with the
    given arguments (same workflow type unless ``workflow`` is given). The
    raised :py:class:`ContinueAsNewError` must not be caught.
    """
    for key, value in {
        "run_timeout": run_timeout,
        "task_timeout": task_timeout,
        "retry_policy": retry_policy,
        "memo": memo,
        "search_attributes": search_attributes,
        "versioning_intent": versioning_intent,
        "initial_versioning_behavior": initial_versioning_behavior,
    }.items():
        if value is not None:
            logger.debug("continue_as_new: ignoring unsupported parameter %r", key)
    _runtime()  # must be called from workflow code
    err = ContinueAsNewError("Workflow continued as new")
    err._tdb_args = _resolve_args(arg, args)
    err._tdb_workflow = (
        _resolve_workflow_type(workflow) if workflow is not None else None
    )
    err._tdb_task_queue = task_queue
    raise err


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

    @staticmethod
    def imports_passed_through() -> "AbstractContextManager[None]":
        """No-op context manager: there is no sandbox to pass imports
        through (DEVIATIONS.md D13)."""
        return nullcontext()

    @staticmethod
    def in_sandbox() -> bool:
        """Always False: there is no workflow sandbox."""
        return False

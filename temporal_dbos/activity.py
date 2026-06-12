"""Activity author API, mirroring ``temporalio.activity``.

Phase 1 surface: the ``defn`` decorator plus the runtime context functions
(``info``, ``heartbeat``, ``is_cancelled``, ``wait_for_cancelled_sync``,
``in_activity``). The context is set by the worker's attempt step for real
runs and by ``temporal_dbos.testing.ActivityEnvironment`` for unit tests.

``heartbeat`` raises CancelledError when cancellation of the activity has
been requested (how sync activities observe cancellation, as in Temporal)
and records details for the next retry attempt — in worker memory, not
durably (DEVIATIONS D6). ``raise_complete_async`` parks the activity for
external completion via ``client.get_async_activity_handle``.
"""

import inspect
import logging
import threading
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import timedelta
from typing import (
    Any,
    Callable,
    Dict,
    List,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    overload,
)

from . import exceptions
from ._internal import registry as _registry

__all__ = [
    "Info",
    "defn",
    "heartbeat",
    "in_activity",
    "info",
    "is_cancelled",
    "logger",
    "raise_complete_async",
    "wait_for_cancelled_sync",
]

_F = TypeVar("_F", bound=Callable[..., Any])

logger = logging.getLogger("temporal_dbos.activity")
"""Logger that can be used within activities. (Phase 1: a plain logger;
the context-injecting adapter mirroring temporalio's lands later.)"""


@overload
def defn(fn: _F) -> _F: ...


@overload
def defn(*, name: Optional[str] = None) -> Callable[[_F], _F]: ...


def defn(
    fn: Optional[_F] = None, *, name: Optional[str] = None
) -> Union[_F, Callable[[_F], _F]]:
    """Decorator for activity functions (sync or async)."""

    def decorator(fn: _F) -> _F:
        defn = _registry.ActivityDefinition(
            name=name if name is not None else fn.__name__,
            fn=fn,
            is_async=inspect.iscoroutinefunction(fn),
        )
        setattr(fn, _registry.ACTIVITY_DEFN_ATTR, defn)
        return fn

    if fn is not None:
        return decorator(fn)
    return decorator


@dataclass(frozen=True)
class Info:
    """Information about the running activity (Phase 1 subset of
    temporalio's ``activity.Info``; field order matches theirs).

    Constructed by the SDK, never by users — the defaults exist only for
    construction convenience.
    """

    activity_id: str = ""
    activity_type: str = ""
    attempt: int = 1
    heartbeat_details: Sequence[Any] = ()
    is_local: bool = False
    task_queue: str = ""
    task_token: bytes = b""
    workflow_id: str = ""
    workflow_run_id: str = ""
    workflow_type: str = ""


@dataclass
class _Context:
    info: Info
    on_heartbeat: Callable[..., None]
    cancelled: threading.Event = field(default_factory=threading.Event)
    last_heartbeat: Sequence[Any] = ()
    # (workflow_run_id, seq) for real runs; None in ActivityEnvironment.
    attempt_key: Optional[Tuple[str, int]] = None


# Worker-process state for in-flight activity attempts, keyed by
# (workflow_run_id, seq). `_live_attempts` lets the interpreter deliver
# cancellation into a running (possibly sync, threaded) attempt;
# `_heartbeat_store` carries last-heartbeat details to the next retry
# attempt (in-memory: Temporal persists these server-side, throttled — a
# durable write per heartbeat is the wrong trade on this hot path);
# `_cancel_requested_keys` covers cancels that land between attempts.
_live_attempts: Dict[Tuple[str, int], "_Context"] = {}
_heartbeat_store: Dict[Tuple[str, int], Sequence[Any]] = {}
_cancel_requested_keys: "set[Tuple[str, int]]" = set()


def _register_attempt(key: Tuple[str, int], ctx: "_Context") -> None:
    _live_attempts[key] = ctx
    if key in _cancel_requested_keys:
        ctx.cancelled.set()


def _unregister_attempt(key: Tuple[str, int]) -> None:
    _live_attempts.pop(key, None)


def _request_cancel(key: Tuple[str, int]) -> None:
    """Deliver a cancellation request into an attempt: observed by the
    activity at its next ``heartbeat()`` (which raises) or via
    ``is_cancelled()``/``wait_for_cancelled_sync()``."""
    _cancel_requested_keys.add(key)
    ctx = _live_attempts.get(key)
    if ctx is not None:
        ctx.cancelled.set()


def _forget_attempt_state(key: Tuple[str, int]) -> None:
    """Drop per-activity worker state once its execution resolves."""
    _heartbeat_store.pop(key, None)
    _cancel_requested_keys.discard(key)


class _CompleteAsyncError(BaseException):
    """Raised by raise_complete_async(); a BaseException (as in temporalio)
    so user ``except Exception`` blocks don't swallow it."""


_current_context: ContextVar[Optional[_Context]] = ContextVar(
    "temporal_dbos_activity", default=None
)


def _context() -> _Context:
    ctx = _current_context.get()
    if ctx is None:
        raise RuntimeError("Not in activity context")
    return ctx


def in_activity() -> bool:
    """Whether the current code is inside an activity."""
    return _current_context.get() is not None


def info() -> Info:
    """Current activity's info."""
    return _context().info


def heartbeat(*details: Any) -> None:
    """Send a heartbeat for the current activity. Details are recorded for
    the next retry attempt's ``info().heartbeat_details`` (in this worker
    process). If cancellation of this activity has been requested, raises
    :py:class:`temporal_dbos.exceptions.CancelledError` — heartbeating is
    how (especially sync) activities observe cancellation, as in Temporal.
    """
    ctx = _context()
    ctx.last_heartbeat = details
    if ctx.attempt_key is not None:
        _heartbeat_store[ctx.attempt_key] = list(details)
    ctx.on_heartbeat(*details)
    if ctx.cancelled.is_set():
        raise exceptions.CancelledError("Activity cancelled")


def raise_complete_async() -> NoReturn:
    """Complete this activity asynchronously: the function returns, but the
    activity stays pending until completed via
    ``client.get_async_activity_handle(task_token=...)``.
    """
    raise _CompleteAsyncError()


def is_cancelled() -> bool:
    """Whether a cancellation was requested on this activity."""
    return _context().cancelled.is_set()


def wait_for_cancelled_sync(
    timeout: Optional[Union[timedelta, float]] = None,
) -> None:
    """Synchronously block until the activity is cancelled."""
    seconds = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
    _context().cancelled.wait(seconds)


def _make_info(meta: dict[str, Any]) -> Info:
    run_id = str(meta.get("workflow_run_id", ""))
    seq = meta.get("seq")
    heartbeat_details: Sequence[Any] = ()
    task_token = b""
    if seq is not None:
        heartbeat_details = tuple(_heartbeat_store.get((run_id, int(seq)), ()))
        # Token format: the seq rides after the last "::" (workflow ids may
        # themselves contain almost anything).
        task_token = f"{run_id}::{int(seq)}".encode()
    return Info(
        activity_id=str(meta.get("activity_id", "")),
        activity_type=str(meta.get("activity_type", "")),
        attempt=int(meta.get("attempt", 1)),
        heartbeat_details=heartbeat_details,
        task_queue=str(meta.get("task_queue", "")),
        task_token=task_token,
        workflow_id=str(meta.get("workflow_id", "")),
        workflow_run_id=run_id,
        workflow_type=str(meta.get("workflow_type", "")),
    )

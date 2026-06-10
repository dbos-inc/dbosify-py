"""Activity author API, mirroring ``temporalio.activity``.

Phase 1 surface: the ``defn`` decorator plus the runtime context functions
(``info``, ``heartbeat``, ``is_cancelled``, ``wait_for_cancelled_sync``,
``in_activity``). The context is set by the worker's attempt step for real
runs and by ``temporal_dbos.testing.ActivityEnvironment`` for unit tests.

Phase 1 notes: ``heartbeat`` records details in-context (and notifies the
test environment's ``on_heartbeat``); durable heartbeat details and
cancellation delivery via heartbeat are Phase 3 (DESIGN §6.1.2).
"""

import inspect
import logging
import threading
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable, List, Optional, Sequence, TypeVar, Union, overload

from ._internal import registry as _registry

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
    temporalio's ``activity.Info``).
    """

    activity_id: str
    activity_type: str
    attempt: int
    task_queue: str
    workflow_id: str
    workflow_run_id: str
    workflow_type: str
    is_local: bool = False
    heartbeat_details: Sequence[Any] = ()


@dataclass
class _Context:
    info: Info
    on_heartbeat: Callable[..., None]
    cancelled: threading.Event = field(default_factory=threading.Event)
    last_heartbeat: Sequence[Any] = ()


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
    """Send a heartbeat for the current activity. Phase 1: details are
    recorded in-context only (durable details and cancellation delivery are
    Phase 3).
    """
    ctx = _context()
    ctx.last_heartbeat = details
    ctx.on_heartbeat(*details)


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
    return Info(
        activity_id=str(meta.get("activity_id", "")),
        activity_type=str(meta.get("activity_type", "")),
        attempt=int(meta.get("attempt", 1)),
        task_queue=str(meta.get("task_queue", "")),
        workflow_id=str(meta.get("workflow_id", "")),
        workflow_run_id=str(meta.get("workflow_run_id", "")),
        workflow_type=str(meta.get("workflow_type", "")),
    )

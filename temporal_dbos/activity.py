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
import json
import logging
import threading
import time as time_mod
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
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
from .common import Priority, RetryPolicy
from .converter import PayloadConverter

if TYPE_CHECKING:
    from .client import Client

_EPOCH = datetime.fromtimestamp(0, timezone.utc)

__all__ = [
    "Info",
    "client",
    "defn",
    "heartbeat",
    "in_activity",
    "info",
    "is_cancelled",
    "is_worker_shutdown",
    "logger",
    "payload_converter",
    "raise_complete_async",
    "shield_thread_cancel_exception",
    "wait_for_cancelled_sync",
    "wait_for_worker_shutdown",
    "wait_for_worker_shutdown_sync",
]


def payload_converter() -> PayloadConverter:
    """The payload converter for this activity (the process's active
    ``DataConverter``'s payload converter), mirroring
    ``temporalio.activity.payload_converter``.

    Use it to convert the ``RawValue`` arguments a dynamic activity receives
    (``payload_converter().from_payload(args[0].payload, MyType)``) or to
    encode/decode interceptor header values (``to_payload``/``from_payload``).
    """
    from ._internal import conversion

    return conversion.get_converter().payload_converter


_F = TypeVar("_F", bound=Callable[..., Any])

logger = logging.getLogger("temporal_dbos.activity")
"""Logger that can be used within activities. (Phase 1: a plain logger;
the context-injecting adapter mirroring temporalio's lands later.)"""


@overload
def defn(fn: _F) -> _F: ...


@overload
def defn(
    *,
    name: Optional[str] = None,
    no_thread_cancel_exception: bool = True,
    dynamic: bool = False,
) -> Callable[[_F], _F]: ...


def defn(
    fn: Optional[_F] = None,
    *,
    name: Optional[str] = None,
    no_thread_cancel_exception: bool = True,
    dynamic: bool = False,
) -> Union[_F, Callable[[_F], _F]]:
    """Decorator for activity functions (sync or async).

    ``dynamic=True`` makes this the catch-all activity, invoked for any
    activity type with no exact registration; it must accept a single
    ``Sequence[RawValue]`` and cannot also set ``name`` (§6.1.2).

    ``no_thread_cancel_exception`` defaults to ``True`` (temporalio's default is
    ``False``): temporal-dbos delivers cancellation to sync activities
    *cooperatively* and never raises into their worker thread, so it always
    behaves as ``True``. Setting it ``False`` — asking for Temporal's
    raise-into-the-thread behavior — raises ``NotImplementedError`` rather than
    silently doing something else (DEVIATIONS D26).
    """
    if name is not None and dynamic:
        raise RuntimeError("Cannot provide name and dynamic boolean")
    if not no_thread_cancel_exception:
        raise NotImplementedError(
            "no_thread_cancel_exception=False (Temporal's default: raise the "
            "cancellation into a sync activity's worker thread) is not "
            "supported — temporal-dbos delivers activity cancellation "
            "cooperatively. Leave it True (the default here) and observe "
            "cancellation via activity.is_cancelled() / activity.heartbeat() / "
            "activity.wait_for_cancelled_sync() (DEVIATIONS D26)."
        )

    def decorator(fn: _F) -> _F:
        from ._internal.conversion import type_hints_from_func

        arg_types, ret_type = type_hints_from_func(fn)
        if dynamic:
            _registry.validate_dynamic_activity_sig(arg_types)
        defn = _registry.ActivityDefinition(
            name=(
                fn.__name__ if dynamic else (name if name is not None else fn.__name__)
            ),
            fn=fn,
            is_async=inspect.iscoroutinefunction(fn),
            arg_types=arg_types,
            ret_type=ret_type,
            dynamic=dynamic,
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
    # In-process activities are dispatched synchronously, so an attempt's
    # schedule/start times coincide (all stamped at attempt execution).
    current_attempt_scheduled_time: datetime = _EPOCH
    heartbeat_details: Sequence[Any] = ()
    heartbeat_timeout: Optional[timedelta] = None
    is_local: bool = False
    namespace: str = "default"
    schedule_to_close_timeout: Optional[timedelta] = None
    scheduled_time: datetime = _EPOCH
    start_to_close_timeout: Optional[timedelta] = None
    started_time: datetime = _EPOCH
    task_queue: str = ""
    task_token: bytes = b""
    workflow_id: str = ""
    # Single-namespace deployment; ``workflow_namespace`` is temporalio's
    # deprecated alias for ``namespace``.
    workflow_namespace: str = "default"
    workflow_run_id: str = ""
    workflow_type: str = ""
    # Priority is accepted-and-inert (FIFO queues); always the default instance.
    priority: Priority = Priority.default
    retry_policy: Optional[RetryPolicy] = None
    # Run ID of this activity; None for workflow-dispatched activities (all ours).
    activity_run_id: Optional[str] = None


@dataclass
class _Context:
    info: Info
    on_heartbeat: Callable[..., None]
    cancelled: threading.Event = field(default_factory=threading.Event)
    last_heartbeat: Sequence[Any] = ()
    last_heartbeat_at: float = field(default_factory=time_mod.monotonic)
    # (workflow_run_id, seq) for real runs; None in ActivityEnvironment.
    attempt_key: Optional[Tuple[str, int]] = None
    # Head of the activity *outbound* interceptor chain (an
    # ActivityOutboundInterceptor), installed per attempt by the worker
    # (DESIGN §6.8). ``info()``/``heartbeat()`` route through it. Left None by
    # ActivityEnvironment, where the functions use the root behavior directly.
    outbound: Optional[Any] = None
    # A Temporal client for ``activity.client()``. ActivityEnvironment may set
    # one explicitly; on real worker runs it is left None and ``client()``
    # lazily builds the process's worker client (see ``_worker_client``).
    client: Optional["Client"] = None


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

# Process-global worker-lifecycle state backing activity.is_worker_shutdown()
# and activity.client(). One Worker per process owns these: the Worker sets the
# config (and clears the shutdown flag) at construction, and sets the shutdown
# flag on shutdown(). ``_worker_client`` is the lazily-built client returned by
# activity.client(); it is destroyed on worker shutdown.
_worker_shutdown_event = threading.Event()
_worker_dbos_config: Optional[Any] = None
_worker_client: Optional["Client"] = None


def _on_worker_start(config: Any) -> None:
    """Called by the Worker at construction: arm the worker-lifecycle state for
    a fresh run (clears any prior shutdown flag / cached client)."""
    global _worker_dbos_config, _worker_client
    _worker_shutdown_event.clear()
    _worker_dbos_config = config
    _worker_client = None


def _on_worker_shutdown() -> None:
    """Called by the Worker on shutdown: trip ``is_worker_shutdown()`` /
    ``wait_for_worker_shutdown*`` and tear down the cached worker client."""
    global _worker_client, _worker_dbos_config
    _worker_shutdown_event.set()
    client = _worker_client
    _worker_client = None
    _worker_dbos_config = None
    if client is not None:
        client._dbos_client.destroy()


def _worker_client_lazy() -> Optional["Client"]:
    """Build (once) and return the process worker's Temporal client, or None if
    no Worker has registered its config in this process."""
    global _worker_client
    if _worker_client is not None:
        return _worker_client
    config = _worker_dbos_config
    if config is None:
        return None
    from dbos import DBOSClient

    from ._internal import conversion
    from .client import Client

    url = config.get("system_database_url") or config.get("database_url")
    dbos_client = DBOSClient(system_database_url=url)
    _worker_client = Client(dbos_client, data_converter=conversion.get_converter())
    return _worker_client


def _register_attempt(key: Tuple[str, int], ctx: "_Context") -> None:
    _live_attempts[key] = ctx
    if key in _cancel_requested_keys:
        # Consume the marker: it must survive until registration because
        # DBOS runs step bodies through its own task/thread machinery — the
        # attempt function can start (detached) AFTER the workflow side
        # cancelled our waiter task and finished its cleanup.
        ctx.cancelled.set()
        _cancel_requested_keys.discard(key)


def _unregister_attempt(key: Tuple[str, int], ctx: "_Context") -> None:
    # Identity-guarded: a cancelled attempt whose unwind outlasts the retry
    # backoff must not pop its successor's registration.
    if _live_attempts.get(key) is ctx:
        del _live_attempts[key]


def _request_cancel(key: Tuple[str, int]) -> None:
    """Deliver a cancellation request into an attempt: observed by the
    activity at its next ``heartbeat()`` (which raises) or via
    ``is_cancelled()``/``wait_for_cancelled_sync()``."""
    _cancel_requested_keys.add(key)
    ctx = _live_attempts.get(key)
    if ctx is not None:
        ctx.cancelled.set()


def _forget_attempt_state(key: Tuple[str, int]) -> None:
    """Drop per-activity worker state once its execution resolves.

    Deliberately does NOT clear a pending cancel-request marker: a
    detached, not-yet-registered attempt must still find it (registration
    consumes it). A marker whose attempt never registers at all leaks a
    small tuple — bounded by cancelled-before-start attempts.
    """
    _heartbeat_store.pop(key, None)


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
    ctx = _context()
    if ctx.outbound is not None:
        info_value: Info = ctx.outbound.info()
        return info_value
    return ctx.info


def heartbeat(*details: Any) -> None:
    """Send a heartbeat for the current activity. Details are recorded for
    the next retry attempt's ``info().heartbeat_details`` (in this worker
    process). If cancellation of this activity has been requested, raises
    :py:class:`temporal_dbos.exceptions.CancelledError` — heartbeating is
    how (especially sync) activities observe cancellation, as in Temporal.
    """
    ctx = _context()
    if ctx.outbound is not None:
        ctx.outbound.heartbeat(*details)
        return
    _root_heartbeat(ctx, *details)


def _root_heartbeat(ctx: _Context, *details: Any) -> None:
    """The un-intercepted heartbeat behavior — the root of the activity
    outbound chain. ``heartbeat()`` calls this directly when no interceptor
    outbound is installed (e.g. ActivityEnvironment); otherwise the chain's
    root outbound calls it."""
    ctx.last_heartbeat = details
    ctx.last_heartbeat_at = time_mod.monotonic()
    if ctx.cancelled.is_set():
        # Raise BEFORE recording to the cross-attempt store: a cancelled
        # attempt's final beat must not re-populate state the workflow side
        # already cleaned up (and its details could never be read anyway —
        # the activity ends cancelled).
        raise exceptions.CancelledError("Activity cancelled")
    if ctx.attempt_key is not None:
        _heartbeat_store[ctx.attempt_key] = list(details)
    ctx.on_heartbeat(*details)


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


def is_worker_shutdown() -> bool:
    """Whether shutdown has been invoked on the worker, mirroring
    ``temporalio.activity.is_worker_shutdown``.

    Raises:
        RuntimeError: When not in an activity.
    """
    _context()  # parity: only valid inside an activity
    return _worker_shutdown_event.is_set()


async def wait_for_worker_shutdown() -> None:
    """Asynchronously wait for shutdown to be called on the worker, mirroring
    ``temporalio.activity.wait_for_worker_shutdown``.

    Raises:
        RuntimeError: When not in an activity.
    """
    _context()
    if _worker_shutdown_event.is_set():
        return
    import asyncio

    await asyncio.get_event_loop().run_in_executor(None, _worker_shutdown_event.wait)


def wait_for_worker_shutdown_sync(
    timeout: Optional[Union[timedelta, float]] = None,
) -> None:
    """Synchronously block until shutdown is called on the worker, mirroring
    ``temporalio.activity.wait_for_worker_shutdown_sync``.

    Raises:
        RuntimeError: When not in an activity.
    """
    _context()
    seconds = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
    _worker_shutdown_event.wait(seconds)


@contextmanager
def shield_thread_cancel_exception() -> Iterator[None]:
    """Context manager for synchronous multithreaded activities to delay
    cancellation exceptions, mirroring
    ``temporalio.activity.shield_thread_cancel_exception``.

    In temporal-dbos this is always a no-op: cancellation is delivered
    cooperatively (via ``is_cancelled()``/``heartbeat()``) and never raised into
    a sync activity's worker thread (DEVIATIONS D26), so there is nothing to
    shield against — matching temporalio's own no-op behavior for async and
    multiprocess activities.
    """
    yield None


def client() -> "Client":
    """Return a Temporal client for use in the current activity, mirroring
    ``temporalio.activity.client``.

    On real worker runs this is the process worker's client (built lazily from
    the Worker's DBOS configuration). In tests it is the client passed to
    :py:class:`temporal_dbos.testing.ActivityEnvironment`.

    Raises:
        RuntimeError: When no client is available.
    """
    ctx = _context()
    available = ctx.client if ctx.client is not None else _worker_client_lazy()
    if available is None:
        raise RuntimeError(
            "No client available. On real worker runs the client is built from "
            "the Worker's configuration; in tests pass a client when creating "
            "ActivityEnvironment."
        )
    return available


def _make_info(meta: dict[str, Any]) -> Info:
    run_id = str(meta.get("workflow_run_id", ""))
    seq = meta.get("seq")
    heartbeat_details: Sequence[Any] = ()
    task_token = b""
    if seq is not None:
        heartbeat_details = tuple(_heartbeat_store.get((run_id, int(seq)), ()))
        # An opaque structured token (workflow ids and activity ids may
        # contain almost anything, so no string separator is safe). On the
        # queued path it also carries the activity workflow id ("qwf"), so an
        # AsyncActivityHandle delivers completion to that workflow's recv rather
        # than the parent run's inbox (§6.1.2).
        token: dict[str, Any] = {
            "run": run_id,
            "aid": str(meta.get("activity_id", "")),
        }
        queued_wf = meta.get("queued_activity_dbos_id")
        if queued_wf:
            token["qwf"] = str(queued_wf)
        task_token = json.dumps(token).encode()
    now = datetime.now(timezone.utc)
    return Info(
        activity_id=str(meta.get("activity_id", "")),
        activity_type=str(meta.get("activity_type", "")),
        attempt=int(meta.get("attempt", 1)),
        current_attempt_scheduled_time=now,
        heartbeat_details=heartbeat_details,
        heartbeat_timeout=_seconds_to_timedelta(meta.get("heartbeat_timeout")),
        schedule_to_close_timeout=_seconds_to_timedelta(meta.get("schedule_to_close")),
        scheduled_time=now,
        start_to_close_timeout=_seconds_to_timedelta(meta.get("start_to_close")),
        started_time=now,
        task_queue=str(meta.get("task_queue", "")),
        task_token=task_token,
        workflow_id=str(meta.get("workflow_id", "")),
        workflow_run_id=run_id,
        workflow_type=str(meta.get("workflow_type", "")),
        retry_policy=_deserialize_retry_policy(meta.get("retry_policy")),
    )


def _seconds_to_timedelta(seconds: Optional[float]) -> Optional[timedelta]:
    return timedelta(seconds=seconds) if seconds is not None else None


def _deserialize_retry_policy(raw: Any) -> Optional[RetryPolicy]:
    if not raw:
        return None
    from ._internal.payloads import deserialize_retry_policy

    return deserialize_retry_policy(raw)

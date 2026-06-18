"""The deterministic interpreter (DESIGN.md §4) — the load-bearing component.

A Temporal workflow is a class: a primary ``run()`` coroutine plus
signal/update/query handlers, multiplexed on a *deterministic* event loop
that only advances when an event arrives. This module hosts that class on a
virtual asyncio loop (a port of temporalio's ``_WorkflowInstance`` machine,
MIT-licensed) inside an async DBOS workflow running on the real loop.

The invariant that makes recovery correct:

    Every nondeterministic fact — what completed, with what result, in what
    order — passes through a DBOS checkpoint before the virtual loop
    observes it.

Event sources and their checkpoints:
  * activities  -> DBOS steps (one per attempt), raced via DBOS.asyncio_wait
  * timers      -> DBOS durable sleeps, raced the same way
  * inbox       -> DBOS.recv on a single per-execution topic (ordered,
                   checkpointed at consumption)
  * the race    -> DBOS.asyncio_wait records the done-set indices per round

Determinism discipline for DBOS function_ids (verified by
tests/integration/test_dbos_semantics.py):
  1. Step wrappers assign their function_id synchronously at call time;
     recv_async/sleep_async/asyncio_wait assign theirs in the coroutine's
     sync prefix (before any await).
  2. The interpreter therefore launches every checkpointed operation at a
     deterministic point of its logical sequence, then yields once
     (``await asyncio.sleep(0)``) so newly created tasks claim function_ids
     in creation order, *before* awaiting the next ``asyncio_wait``.
  3. Replay re-runs the same logical sequence, so every operation lands on
     the function_id that holds its checkpoint. Gaps from operations that
     never recorded (cancelled in-flight steps) are harmless: function_ids
     order lookups but need not be dense.
"""

import asyncio
import json
import logging
import os
import secrets
import time as time_mod
import warnings
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from random import Random
from typing import (
    Any,
    Callable,
    Coroutine,
    Deque,
    Dict,
    List,
    Mapping,
    NoReturn,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from dbos import DBOS
from dbos._context import get_local_dbos_context  # see docs/phase0.md
from dbos._error import DBOSUnexpectedStepError
from dbos._utils import GlobalParams  # the worker's live DBOS application_version

from .. import activity as activity_api
from .. import exceptions
from ..common import (
    RawValue,
    RetryPolicy,
    SearchAttributes,
    SearchAttributeUpdate,
    TypedSearchAttributes,
    WorkerDeploymentVersion,
)
from ..workflow import (
    ActivityHandle,
    ChildWorkflowHandle,
    ContinueAsNewError,
    HandlerUnfinishedPolicy,
    Info,
    NondeterminismError,
    ParentInfo,
    ReadOnlyContextError,
    RootInfo,
    UnfinishedSignalHandlersWarning,
    UnfinishedUpdateHandlersWarning,
    UpdateInfo,
    _current_update_info,
    _Runtime,
)
from . import activities as activities_mod
from . import attributes as _attributes
from . import conversion, ids, inbox
from . import replay as _replay
from . import workflow_interceptor as _wfi
from .payloads import (
    FailureEnvelope,
    RunMeta,
    deserialize_failure,
    deserialize_retry_policy,
    serialize_retry_policy,
    wrap_input,
)
from .registry import WorkflowDefinition

logger = logging.getLogger("temporal_dbos.interpreter")


class WorkflowTaskFailure(Exception):
    """Internal: a non-failure exception escaped workflow code. In Temporal
    this fails the *workflow task*, not the workflow: the dispatcher logs,
    backs off, and re-runs the interpreter from checkpoints while the
    workflow stays RUNNING (DESIGN.md §4.2).
    """

    def __init__(self, cause: BaseException) -> None:
        super().__init__(f"workflow task failed: {cause!r}")
        self.cause = cause


class WorkflowContinuedAsNew(Exception):
    """Internal: the run ended via continue-as-new; the next run is already
    enqueued. The dispatcher converts this into the SerializedContinueAsNew
    marker (the chain-hop analog of the cancellation marker)."""

    def __init__(self, new_run_id: str) -> None:
        super().__init__(new_run_id)
        self.new_run_id = new_run_id


class WorkflowCancelled(Exception):
    """Internal: the workflow ended via cooperative cancellation. The
    dispatcher converts this into the cancelled marker (DESIGN §6.5)."""

    def __init__(self, cause: BaseException) -> None:
        super().__init__(repr(cause))
        self.cause = cause


class _AbortDrain(Exception):
    """Internal: an exception escaped a non-task callback (e.g. a
    wait_condition predicate) during a virtual-loop drain.
    """

    def __init__(self, cause: BaseException) -> None:
        super().__init__(repr(cause))
        self.cause = cause


# How often the child-result step polls for the child's terminal state.
CHILD_POLL_INTERVAL_SECONDS = 0.25

# is_continue_as_new_suggested() threshold on the run's checkpoint count
# (Temporal's server suggests around 10k history events).
CAN_SUGGESTION_THRESHOLD = int(
    os.environ.get("TEMPORAL_DBOS_CAN_SUGGESTION_THRESHOLD", "10000")
)

# Created lazily (not at import) so decoration binds to the live DBOS
# registry — tests destroy and re-create it between cases.
_init_step: Optional[Callable[[], Any]] = None
_child_result_step: Optional[Callable[[str], Any]] = None
_child_exists_step: Optional[Callable[[str], Any]] = None
_activity_result_step: Optional[Callable[[str], Any]] = None
_update_validate_step: Optional[Callable[[Callable[[], None]], Any]] = None
_safe_status_step: Optional[Callable[[str], Any]] = None
_safe_status_list_step: Optional[Callable[[List[str]], Any]] = None
_patch_step: Optional[Callable[[str], Any]] = None

# The DBOS step name for a workflow.patched()/deprecate_patch() marker. The
# step's recorded output is the patch id, so a recovering run rebuilds the set
# of recorded patch ids by scanning its step list for this name (see
# Interpreter.execute). Mirrors temporalio's SetPatchMarker, but keyed by id
# (set membership), not by position — robust to code that shifts checkpoints.
PATCH_STEP_NAME = "__tdb_patch"


def _workflow_init_step() -> Any:
    """One checkpoint per execution for every per-execution nondeterministic
    constant: the virtual clock's start time and the randomness seed.
    """
    global _init_step
    if _init_step is None:

        @DBOS.step(name="__tdb_init")
        async def init_step() -> Dict[str, Any]:
            return {"start_time": time_mod.time(), "seed": secrets.randbits(63)}

        _init_step = init_step
    return _init_step()


def _validate_update(validate: Callable[[], None]) -> Any:
    """Checkpoint the update validator's verdict so the validator runs
    exactly once: replay returns the recorded verdict without re-executing
    it. This is Temporal's semantics — acceptance is recorded in history and
    validators are skipped on replay — so a nondeterministic validator
    cannot flip its verdict and corrupt the execution. The callable argument
    is never serialized (steps record only their results).
    """
    global _update_validate_step
    if _update_validate_step is None:

        @DBOS.step(name="__tdb_upd_validate")
        async def update_validate_step(validate: Callable[[], None]) -> Dict[str, Any]:
            from .payloads import serialize_failure

            try:
                validate()
            except BaseException as err:  # noqa: BLE001
                return {"accepted": False, "failure": serialize_failure(err)}
            return {"accepted": True}

        _update_validate_step = update_validate_step
    return _update_validate_step(validate)


def _await_child_result(child_id: str) -> Any:
    """The child-result waiter: our own step wrapping the *non-recording*
    wait, returning an envelope.

    DBOS's handle.get_result() is unusable inside racing waiter tasks: its
    record_get_result claims a function_id at COMPLETION time (completion
    order = nondeterministic), and it never reads the record back. Wrapping
    the raw await in our own step claims the function_id at launch (like
    activity attempts) and records the child's outcome in our slot.
    """
    global _child_result_step
    if _child_result_step is None:

        @DBOS.step(name="__tdb_child_result")
        async def child_result_step(child_id: str) -> Dict[str, Any]:
            from dbos._dbos import _get_dbos_instance  # see docs/phase0.md
            from dbos._error import DBOSAwaitedWorkflowCancelledError

            from .payloads import (
                SerializedContinueAsNew,
                SerializedWorkflowCancellation,
                SerializedWorkflowFailure,
                serialize_failure,
            )

            dbos = _get_dbos_instance()
            try:
                while True:
                    try:
                        result = await dbos._sys_db.await_workflow_result_async(
                            child_id, CHILD_POLL_INTERVAL_SECONDS
                        )
                        break
                    except SerializedContinueAsNew as marker:
                        # The child continued as new: its result is the
                        # final run's (Temporal semantics) — follow the
                        # chain. The step records only the final outcome.
                        child_id = marker.envelope["new_run_id"]
                    except SerializedWorkflowFailure as failed:
                        # A failed run with a successor (workflow retry from the
                        # child's retry_policy) is followed to that successor,
                        # exactly as the client's result(follow_runs=True) and
                        # temporalio's new_execution_run_id do. A terminal
                        # failure (no successor) propagates to the handler below.
                        successor = failed.envelope.get("new_run_id")
                        if successor is None:
                            raise
                        child_id = successor
            except SerializedWorkflowCancellation as cancelled:
                return {
                    "ok": False,
                    "cancelled": True,
                    "failure": cancelled.envelope,
                    "ended_at": time_mod.time(),
                }
            except SerializedWorkflowFailure as failed:
                return {
                    "ok": False,
                    "failure": failed.envelope,
                    "ended_at": time_mod.time(),
                }
            except DBOSAwaitedWorkflowCancelledError:
                # Native DBOS cancel == the child was terminated.
                terminated = exceptions.TerminatedError("Child workflow terminated")
                return {
                    "ok": False,
                    "failure": serialize_failure(terminated),
                    "ended_at": time_mod.time(),
                }
            except Exception as err:  # noqa: BLE001 — FAIL_FAST / legacy errors
                return {
                    "ok": False,
                    "failure": serialize_failure(err),
                    "ended_at": time_mod.time(),
                }
            return {"ok": True, "result": result, "ended_at": time_mod.time()}

        _child_result_step = child_result_step
    return _child_result_step(child_id)


def _child_id_taken(child_id: str) -> Any:
    """Checkpointed existence check for a child workflow id, so the answer
    is replay-stable: on first execution the child doesn't exist yet (we
    haven't started it); on replay the recorded False is returned even
    though the child now exists *because we created it*. A live read here
    would spuriously fail every replay.
    """
    global _child_exists_step
    if _child_exists_step is None:

        @DBOS.step(name="__tdb_child_check")
        async def child_exists_step(child_id: str) -> bool:
            status = await DBOS.get_workflow_status_async(child_id)
            return status is not None

        _child_exists_step = child_exists_step
    return _child_exists_step(child_id)


def _await_activity_result(activity_id: str) -> Any:
    """The queued-activity result waiter (the cross-queue path, §6.1.2): our
    own step wrapping the non-recording wait on the ``__temporal_activity``
    workflow, returning its envelope. Same rationale as ``_await_child_result``
    — DBOS's ``get_result`` claims its function_id at completion (a
    nondeterministic position across racing waiters), so we wrap the raw await
    in a step that claims the id at launch and records the outcome in our slot.

    The activity workflow catches user exceptions and returns an envelope
    (``ok=False``) rather than raising, so the only exceptions reaching here are
    termination (native DBOS cancel of the activity workflow) or an
    infrastructure error.
    """
    global _activity_result_step
    if _activity_result_step is None:

        @DBOS.step(name="__tdb_activity_result")
        async def activity_result_step(activity_id: str) -> Dict[str, Any]:
            from dbos._dbos import _get_dbos_instance  # see docs/phase0.md
            from dbos._error import DBOSAwaitedWorkflowCancelledError

            from .payloads import serialize_failure

            dbos = _get_dbos_instance()
            try:
                envelope: Dict[str, Any] = (
                    await dbos._sys_db.await_workflow_result_async(
                        activity_id, CHILD_POLL_INTERVAL_SECONDS
                    )
                )
            except DBOSAwaitedWorkflowCancelledError:
                # The activity workflow was terminated (native DBOS cancel):
                # surface it as a cancellation of the activity.
                cancelled = exceptions.CancelledError("Activity cancelled")
                return {
                    "ok": False,
                    "failure": serialize_failure(cancelled),
                    "ended_at": time_mod.time(),
                }
            except Exception as err:  # noqa: BLE001 — FAIL_FAST / infra errors
                return {
                    "ok": False,
                    "failure": serialize_failure(err),
                    "ended_at": time_mod.time(),
                }
            return envelope

        _activity_result_step = activity_result_step
    return _activity_result_step(activity_id)


# The status fields the interpreter actually reads — all JSON-serializable.
# A whole WorkflowStatus is not JSON-safe (it embeds input/output/error), so
# these steps checkpoint only what we use; the checkpoint keeps the read
# replay-stable (same as a direct status read, just serializable).
def _safe_status(workflow_id: str) -> Any:
    """Checkpointed read of a workflow's JSON-safe status fields, or None."""
    global _safe_status_step
    if _safe_status_step is None:

        @DBOS.step(name="__tdb_status")
        async def safe_status_step(workflow_id: str) -> Optional[Dict[str, Any]]:
            status = await DBOS.get_workflow_status_async(workflow_id)
            if status is None:
                return None
            return {
                "status": status.status,
                "queue_name": status.queue_name,
                "parent_workflow_id": status.parent_workflow_id,
            }

        _safe_status_step = safe_status_step
    return _safe_status_step(workflow_id)


def _safe_status_list(dbos_ids: List[str]) -> Any:
    """Checkpointed batched probe returning ``{dbos_id: status_string}`` for
    existing ids (chain resolution; the caller uses only id/status)."""
    global _safe_status_list_step
    if _safe_status_list_step is None:

        @DBOS.step(name="__tdb_status_list")
        async def safe_status_list_step(dbos_ids: List[str]) -> Dict[str, str]:
            statuses = await DBOS.list_workflows_async(workflow_ids=dbos_ids)
            return {s.workflow_id: s.status for s in statuses}

        _safe_status_list_step = safe_status_list_step
    return _safe_status_list_step(dbos_ids)


def _patch_marker(patch_id: str) -> Any:
    """Record a ``workflow.patched()`` marker as a checkpointed step whose
    output is the patch id. Claimed at a deterministic position (command order,
    awaited inline in ``_process_commands`` — same model as the ``send`` /
    ``attributes`` commands), so replay reads the id back, and the next run's
    step-list scan rediscovers it. The body never re-runs on replay.
    """
    global _patch_step
    if _patch_step is None:

        @DBOS.step(name=PATCH_STEP_NAME)
        async def patch_step(patch_id: str) -> str:
            return patch_id

        _patch_step = patch_step
    return _patch_step(patch_id)


class _TimerHandle(asyncio.TimerHandle):
    def __init__(
        self,
        seq: int,
        when: float,
        callback: Callable[..., Any],
        args: Sequence[Any],
        loop: "_VirtualLoop",
        context: Any = None,
    ) -> None:
        super().__init__(when, callback, args, loop, context)
        self.seq = seq


class _VirtualLoop(asyncio.AbstractEventLoop):
    """The deterministic loop hosting workflow coroutines. It never blocks:
    ``Interpreter._drain`` runs ready callbacks until everything is parked,
    and event delivery (from checkpointed sources) is what un-parks them.
    """

    def __init__(self, interpreter: "Interpreter") -> None:
        self._interpreter = interpreter
        self.ready: Deque[asyncio.Handle] = deque()
        self.time_seconds = 0.0
        # Read by workflow.py's _runtime() via asyncio.get_running_loop().
        self.tdb_runtime = interpreter

    # -- scheduling callbacks -------------------------------------------------

    def call_soon(  # type: ignore[override]
        self, callback: Callable[..., Any], *args: Any, context: Any = None
    ) -> asyncio.Handle:
        handle = asyncio.Handle(callback, args, self, context)
        self.ready.append(handle)
        return handle

    def call_later(  # type: ignore[override]
        self,
        delay: float,
        callback: Callable[..., Any],
        *args: Any,
        context: Any = None,
    ) -> asyncio.TimerHandle:
        return self._interpreter._create_timer(float(delay), callback, args, context)

    def call_at(  # type: ignore[override]
        self,
        when: float,
        callback: Callable[..., Any],
        *args: Any,
        context: Any = None,
    ) -> asyncio.TimerHandle:
        return self.call_later(when - self.time(), callback, *args, context=context)

    def _timer_handle_cancelled(self, handle: asyncio.TimerHandle) -> None:
        assert isinstance(handle, _TimerHandle)
        self._interpreter._cancel_timer(handle)

    def time(self) -> float:
        return self.time_seconds

    # -- futures and tasks ----------------------------------------------------

    def create_future(self) -> "asyncio.Future[Any]":
        return asyncio.Future(loop=self)

    def create_task(
        self,
        coro: Any,
        *,
        name: Optional[str] = None,
        context: Any = None,
        eager_start: Optional[bool] = None,
    ) -> "asyncio.Task[Any]":
        self._interpreter._assert_not_read_only("create task")
        # eager_start (3.14+) is ignored: a task on the virtual loop must not
        # run any code until the interpreter drains the ready queue.
        task: asyncio.Task[Any] = asyncio.Task(coro, loop=self, name=name)
        self._interpreter._tasks.add(task)
        task.add_done_callback(self._interpreter._tasks.discard)
        return task

    # -- misc loop protocol ---------------------------------------------------

    def get_debug(self) -> bool:
        return False

    def is_running(self) -> bool:
        return True

    def is_closed(self) -> bool:
        return False

    def call_exception_handler(self, context: Dict[str, Any]) -> None:
        # Mirrors temporalio: unhandled errors from non-task callbacks are
        # rare; surface them in logs rather than crashing the host loop.
        logger.error(
            "Workflow virtual loop exception: %s",
            context.get("message"),
            exc_info=context.get("exception"),
        )

    def default_exception_handler(self, context: Dict[str, Any]) -> None:
        self.call_exception_handler(context)


@dataclass
class _Waiter:
    """A pending real-loop task the next event may come from."""

    kind: str  # "inbox" | "timer" | "activity"
    seq: int
    task: "asyncio.Task[Any]"


@dataclass
class _ActivityExec:
    """Per-activity retry state machine (DESIGN.md §6.1.2): each attempt is
    its own DBOS step, each backoff its own durable sleep — both launched
    only from checkpointed event-delivery points so recovery resumes at the
    right attempt.
    """

    seq: int
    activity_name: str
    args: List[Any]
    retry_policy: RetryPolicy
    start_to_close: Optional[float]
    schedule_to_close: Optional[float]
    activity_id: str
    scheduled_at: float  # virtual time when scheduled
    future: "asyncio.Future[Any]"  # on the virtual loop
    cancellation_type: int = 0  # ActivityCancellationType; 2 = ABANDON
    attempt: int = 1
    in_backoff: bool = False
    last_failure: Optional[FailureEnvelope] = None
    cancel_requested: bool = False  # WAIT_CANCELLATION_COMPLETED in flight
    async_pending: bool = False  # raise_complete_async(): awaiting external
    heartbeat_timeout: Optional[float] = None
    # Whether a completer heartbeat arrived within the current watch window
    # (the parked heartbeat-timeout check counts envelopes between timer
    # fires — deterministic, no clock reads).
    async_hb_seen: bool = False
    # execute_activity(result_type=...) override; falls back to the activity's
    # registered return annotation.
    result_type: Optional[type] = None
    # execute_activity(task_queue=...): when set and different from the
    # workflow's own queue, the activity runs on another worker via the
    # ``__temporal_activity`` queued path (§6.1.2) instead of a local step.
    task_queue: Optional[str] = None
    # The ``__temporal_activity`` workflow id once dispatched on the queued path
    # (None on the local path); cancellation natively cancels this workflow.
    queued_dbos_id: Optional[str] = None
    # schedule_to_start_timeout in seconds; only meaningful on the queued path
    # (the local path has no queue wait), where it bounds the queue dwell.
    schedule_to_start: Optional[float] = None
    # Interceptor headers in wire form (str -> payload dict), set by the
    # outbound chain; delivered to the activity attempt as ExecuteActivityInput.
    headers: Dict[str, Any] = field(default_factory=dict)


@dataclass
class _ChildExec:
    """Per-child-workflow state. The start resolves once the enqueue is
    durable (Temporal: handles resolve on start); the result arrives through
    the event race via the child-result step.
    """

    seq: int
    type_name: str
    child_id: str
    args: List[Any]
    task_queue: Optional[str]
    parent_close_policy: int  # ParentClosePolicy
    cancellation_type: int  # ChildWorkflowCancellationType
    start_future: "asyncio.Future[Any]"  # on the virtual loop
    result_future: "asyncio.Future[Any]"  # on the virtual loop
    # Raw memo / search attributes for the child (encoded on the real loop in
    # _start_child); children do NOT inherit the parent's, matching Temporal.
    memo: Optional[Mapping[str, Any]] = None
    search_attributes: Optional[Union[TypedSearchAttributes, SearchAttributes]] = None
    started: bool = False
    # Interceptor headers in wire form (str -> payload dict), set by the
    # outbound chain; delivered to the child run as ExecuteWorkflowInput.headers.
    headers: Dict[str, Any] = field(default_factory=dict)
    # Per-run timeout (seconds) and serialized RetryPolicy for the child,
    # carried in its RunMeta exactly like a top-level start: run_timeout drives
    # SetWorkflowTimeout on the enqueue and re-applies across the child's chain;
    # retry_policy gates the child's own workflow retries (the child-result step
    # follows the resulting chain, so the parent still sees the final outcome).
    run_timeout: Optional[float] = None
    retry_policy: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------------------------
# Workflow interceptor chain roots (DEVIATIONS D24). Built fresh per execution
# in Interpreter._build_interceptor_chains; the inbound root performs the real
# dispatch into user handlers, the outbound root performs the real activity /
# child / signal / continue-as-new operations via the interpreter's runtime
# methods. User interceptors (from Worker(interceptors=...)) wrap these.
# ---------------------------------------------------------------------------


class _RootWorkflowInbound(_wfi.WorkflowInboundInterceptor):
    """Root of the workflow inbound chain: invokes the user's run / signal /
    query / update handlers. ``init`` installs the (possibly wrapped) outbound
    on the interpreter so workflow outbound calls route through it."""

    def __init__(self, interp: "Interpreter") -> None:
        # Chain root: no ``next`` to delegate to.
        self._interp = interp

    def init(self, outbound: _wfi.WorkflowOutboundInterceptor) -> None:
        self._interp._outbound = outbound

    async def execute_workflow(self, input: _wfi.ExecuteWorkflowInput) -> Any:
        return await input.run_fn(self._interp._instance, *input.args)

    async def handle_signal(self, input: _wfi.HandleSignalInput) -> None:
        # Exact match first, then the dynamic (catch-all) handler — a dynamic
        # handler is keyed ``None`` and called as ``fn(self, name, args)`` (the
        # args were shaped to ``[name, Sequence[RawValue]]`` in _decode).
        signals = self._interp._defn.signals
        defn = signals.get(input.signal) or signals.get(None)
        if defn is None:  # pragma: no cover — _apply_signal resolved it
            return
        result = defn.fn(self._interp._instance, *input.args)
        if asyncio.iscoroutine(result):
            await result

    async def handle_query(self, input: _wfi.HandleQueryInput) -> Any:
        queries = self._interp._defn.queries
        defn = queries.get(input.query) or queries.get(None)
        assert defn is not None  # _apply_query resolved it before routing
        return defn.fn(self._interp._instance, *input.args)

    def handle_update_validator(self, input: _wfi.HandleUpdateInput) -> None:
        updates = self._interp._defn.updates
        defn = updates.get(input.update) or updates.get(None)
        assert defn is not None  # _apply_update resolved it before routing
        if defn.validator is None:  # pragma: no cover — only routed when set
            return
        # Synchronous, read-only, against current state; a rejected update
        # must leave no trace in workflow state.
        self._interp._read_only = True
        try:
            defn.validator(self._interp._instance, *input.args)
        finally:
            self._interp._read_only = False

    async def handle_update_handler(self, input: _wfi.HandleUpdateInput) -> Any:
        updates = self._interp._defn.updates
        defn = updates.get(input.update) or updates.get(None)
        assert defn is not None  # _apply_update resolved it before routing
        result = defn.fn(self._interp._instance, *input.args)
        if asyncio.iscoroutine(result):
            result = await result
        return result


class _RootWorkflowOutbound(_wfi.WorkflowOutboundInterceptor):
    """Root of the workflow outbound chain: performs the real activity / child /
    signal / continue-as-new operations via the interpreter's runtime methods."""

    def __init__(self, interp: "Interpreter") -> None:
        self._interp = interp

    def continue_as_new(self, input: _wfi.ContinueAsNewInput) -> "NoReturn":
        err = ContinueAsNewError("Workflow continued as new")
        err._tdb_args = list(input.args)
        err._tdb_workflow = input.workflow
        err._tdb_task_queue = input.task_queue
        err._tdb_run_timeout = input.run_timeout
        err._tdb_retry_policy = input.retry_policy
        err._tdb_memo = input.memo
        err._tdb_search_attributes = input.search_attributes
        # Raw Payloads; codec-encoded on the real loop in _begin_continue_as_new
        # (this runs on the virtual loop, which can't await the async codec).
        err._tdb_headers = dict(input.headers)
        raise err

    def info(self) -> Info:
        return self._interp.runtime_info()

    async def signal_child_workflow(self, input: _wfi.SignalChildWorkflowInput) -> None:
        # Raw-Payload headers ride in the envelope; the send command encodes them
        # (with args) on the real loop in _process_commands.
        envelope = inbox.signal_envelope(
            input.signal,
            list(input.args),
            headers=dict(input.headers),
        )
        # The child may have continued as new; resolve to its current run.
        await self._interp.runtime_send_to_workflow(
            input.child_workflow_id, envelope, resolve_chain=True
        )

    async def signal_external_workflow(
        self, input: _wfi.SignalExternalWorkflowInput
    ) -> None:
        envelope = inbox.signal_envelope(
            input.signal,
            list(input.args),
            headers=dict(input.headers),
        )
        await self._interp.runtime_send_to_workflow(
            input.workflow_run_id or input.workflow_id,
            envelope,
            resolve_chain=input.workflow_run_id is None,
        )

    def start_activity(self, input: _wfi.StartActivityInput) -> ActivityHandle:
        return self._interp.runtime_start_activity(
            input.activity,
            list(input.args),
            schedule_to_close_timeout=input.schedule_to_close_timeout,
            start_to_close_timeout=input.start_to_close_timeout,
            retry_policy=input.retry_policy,
            activity_id=input.activity_id,
            cancellation_type=int(input.cancellation_type),
            heartbeat_timeout=input.heartbeat_timeout,
            result_type=input.ret_type,
            task_queue=input.task_queue,
            schedule_to_start_timeout=input.schedule_to_start_timeout,
            # Raw Payloads; codec-encoded on the real loop in _process_commands.
            headers=input.headers,
        )

    def start_local_activity(
        self, input: _wfi.StartLocalActivityInput
    ) -> ActivityHandle:
        return self._interp.runtime_start_activity(
            input.activity,
            list(input.args),
            schedule_to_close_timeout=input.schedule_to_close_timeout,
            start_to_close_timeout=input.start_to_close_timeout,
            retry_policy=input.retry_policy,
            activity_id=input.activity_id,
            cancellation_type=int(input.cancellation_type),
            result_type=input.ret_type,
            headers=input.headers,
        )

    async def start_child_workflow(
        self, input: _wfi.StartChildWorkflowInput
    ) -> ChildWorkflowHandle:
        return await self._interp.runtime_start_child_workflow(
            input.workflow,
            list(input.args),
            # An empty id means "auto" (the interpreter derives {parent}_{seq});
            # workflow.start_child_workflow passes "" when the caller gave no id.
            child_id=input.id or None,
            task_queue=input.task_queue,
            parent_close_policy=int(input.parent_close_policy),
            cancellation_type=int(input.cancellation_type),
            memo=input.memo,
            search_attributes=input.search_attributes,
            run_timeout=input.run_timeout,
            retry_policy=input.retry_policy,
            # Raw Payloads; codec-encoded on the real loop in _start_child.
            headers=input.headers,
        )


class Interpreter(_Runtime):
    """Hosts one workflow execution. Construct fresh for each (re)execution;
    ``execute()`` replays deterministically from whatever checkpoints exist.
    """

    def __init__(
        self,
        defn: WorkflowDefinition,
        args: Sequence[Any],
        meta: Optional[RunMeta] = None,
    ) -> None:
        self._defn = defn
        self._args = list(args)
        self._meta = meta if meta is not None else RunMeta()
        # AbstractEventLoop stubs mark the loop protocol abstract; we
        # implement the subset workflow coroutines exercise (the same
        # approach temporalio's _WorkflowInstance takes).
        self._vloop = _VirtualLoop(self)  # type: ignore[abstract]
        self._instance: Any = None
        self._primary_task: Optional["asyncio.Task[Any]"] = None
        self._tasks: Set["asyncio.Task[Any]"] = set()
        self._conditions: List[Tuple[Callable[[], bool], "asyncio.Future[Any]"]] = []
        self._seqs: Dict[str, int] = {}
        self._pending_timers: Dict[int, _TimerHandle] = {}
        self._pending_activities: Dict[int, _ActivityExec] = {}
        self._commands: List[Tuple[str, int]] = []
        self._outbox: List[Tuple[str, Any]] = []
        self._waiters: List[_Waiter] = []
        self._buffered_signals: Dict[str, List[inbox.Envelope]] = {}
        self._seen_update_ids: Set[str] = set()
        # Live signal/update handler tasks -> {kind, name, id, policy};
        # backs all_handlers_finished() and the unfinished-handler warnings.
        self._inflight_handlers: "Dict[asyncio.Task[Any], Dict[str, Any]]" = {}
        self._read_only = False
        self._cancel_requested = False
        self._cancel_reason: Optional[str] = None
        self._cancelled_activity_seqs: List[int] = []
        # Queued-activity WAIT_CANCELLATION_COMPLETED requests: the cancel event
        # is set in the (async) sweep, but the exec stays open until the
        # activity confirms its unwind via the result step.
        self._queued_wait_cancel_seqs: List[int] = []
        self._abandoned_tasks: Set["asyncio.Task[Any]"] = set()
        self._pending_children: Dict[int, _ChildExec] = {}
        self._children_registry: List[Dict[str, Any]] = []
        self._cancelled_child_seqs: List[int] = []
        self._pending_sends: Dict[int, Tuple[str, Any, "asyncio.Future[Any]", bool]] = (
            {}
        )
        self._own_queue_name: Optional[str] = None
        self._own_queue_resolved = False
        # This process's Worker task queue (one Worker per process): the queue
        # this workflow runs on, surfaced as info().task_queue and as a local
        # activity's task_queue. "default" under the in-process Phase-0 harness.
        from . import registry

        self._task_queue_name = registry.worker_task_queue or "default"
        self._replay_horizon = 0
        # workflow.patched()/deprecate_patch() state (DESIGN §6.8). Patch ids
        # whose marker exists in recorded history (rebuilt from the step list at
        # execute() start); the per-id decision memo (one decision per id per
        # run, like temporalio); and markers queued for durable write this turn.
        self._patches_recorded: Set[str] = set()
        self._patches_memoized: Dict[str, bool] = {}
        self._pending_patches: Dict[int, str] = {}
        # Set when a rehydrate (query-on-closed replay) scratch run is told to
        # stop serving queries and complete (see _serve loop in execute()).
        self._rehydrate_stop = False
        self._rehydrate_deadline: Optional[float] = None
        self._can_new_run_id: Optional[str] = None
        self._continued_from: Optional[str] = None
        # The DBOS run id of a real (cross-chain) parent workflow, set at start
        # when this run was launched as a child; None for top-level/continued
        # runs. Surfaced as workflow.info().parent.
        self._parent_run_id: Optional[str] = None
        # Decoded memo + search attributes for this run: materialized from the
        # run envelope at start, mutated in place by upsert_*, and the source
        # for in-workflow info()/memo() (the durable, queryable copy lives in
        # the DBOS attributes column).
        self._memo: Dict[str, Any] = {}
        self._typed_sa: TypedSearchAttributes = TypedSearchAttributes.empty
        # Free-form UI/CLI details set via workflow.set_current_details(): pure
        # in-memory state, reconstructed deterministically on recovery by
        # replaying the same set_current_details calls (no checkpoint needed —
        # not surfaced to describe()/list in v1, DEVIATIONS D30).
        self._current_details: str = ""
        self._random = Random(0)
        # The deterministic random seed (checkpointed once at run start), exposed
        # via workflow.random_seed(); fixed for the run's lifetime.
        self._seed: int = 0
        self._workflow_id = ""
        self._start_time = 0.0
        # ("ok", result) | ("failure", exc) | ("task_failure", exc)
        self._outcome: Optional[Tuple[str, Any]] = None
        # Workflow interceptor chains (DEVIATIONS D24), built in execute().
        # _inbound wraps run/handler dispatch; _outbound (installed via
        # _inbound.init) wraps activity/child/signal/continue-as-new calls.
        self._inbound: Optional[_wfi.WorkflowInboundInterceptor] = None
        self._outbound: Optional[_wfi.WorkflowOutboundInterceptor] = None
        # The run's interceptor headers, decoded to Payloads in execute().
        self._headers: Mapping[str, Any] = {}

    def _build_interceptor_chains(self) -> None:
        """Build the inbound/outbound interceptor chains for this execution from
        the worker's registered interceptors (mirroring temporalio's per-execution
        construction). With no interceptors this is just the roots, preserving the
        prior dispatch exactly."""
        from . import registry

        inbound: _wfi.WorkflowInboundInterceptor = _RootWorkflowInbound(self)
        for interceptor in reversed(registry.worker_interceptors):
            cls = interceptor.workflow_interceptor_class(
                _wfi.WorkflowInterceptorClassInput(unsafe_extern_functions={})
            )
            if cls is not None:
                inbound = cls(inbound)
        self._inbound = inbound
        # init() walks the chain installing the (possibly wrapped) outbound; the
        # root inbound's init stores the final outbound on self._outbound.
        inbound.init(_RootWorkflowOutbound(self))

    # ------------------------------------------------------------------
    # The outer loop (real asyncio loop, inside the DBOS workflow)
    # ------------------------------------------------------------------

    async def execute(self) -> Any:
        ctx = get_local_dbos_context()
        assert ctx is not None, "interpreter must run inside a DBOS workflow"
        self._workflow_id = ctx.workflow_id

        # The checkpoint horizon: the highest recorded function_id. While our
        # claim cursor is below it, we are re-executing recorded history —
        # that is what unsafe.is_replaying() reports. The read must be live
        # (is_replaying is *about* replay state, exempt from determinism),
        # but inside a workflow context DBOS checkpoints listWorkflowSteps
        # itself — it would replay its own first-execution (empty) result.
        # An executor thread has no DBOS context, so the read stays live.
        # (Upstream wishlist: a cheap max-function_id query; this fetches
        # and deserializes the full step list.)
        steps = await asyncio.get_running_loop().run_in_executor(
            None, DBOS.list_workflow_steps, self._workflow_id
        )
        self._replay_horizon = max((step["function_id"] for step in steps), default=0)

        # Rebuild the set of patch ids whose marker is in recorded history, by
        # id (set membership), not by position — so patched() returns the same
        # verdict on replay regardless of checkpoint shifts (DESIGN §6.8). Each
        # marker step's recorded output is its patch id.
        self._patches_recorded = {
            step["output"]
            for step in steps
            if step["function_name"] == PATCH_STEP_NAME and step["output"] is not None
        }

        # continued_run_id: DBOS threads parent_workflow_id automatically for
        # in-workflow starts, and our continue-as-new enqueue runs inside the
        # closing run — so a parent link *within the same chain* is exactly a
        # continuation link (a real parent points at a different chain base;
        # client-side reuse starts carry no link). Immutable for the run, so
        # a live read is replay-safe (executor thread: in-context the call
        # would be checkpointed, which is unnecessary here).
        status = await asyncio.get_running_loop().run_in_executor(
            None, DBOS.get_workflow_status, self._workflow_id
        )
        parent = status.parent_workflow_id if status else None
        if parent is not None:
            if ids.parse_run(parent)[0] == ids.parse_run(self._workflow_id)[0]:
                self._continued_from = parent
            else:
                # A link to a different chain base is a real parent (a child
                # start), not a continuation.
                self._parent_run_id = parent

        init = await _workflow_init_step()
        self._start_time = float(init["start_time"])
        self._vloop.time_seconds = self._start_time
        self._seed = int(init["seed"])
        self._random.seed(self._seed)

        # Rebuild the typed run arguments from their payloads (deterministic,
        # so re-decoding each run/replay is replay-safe).
        self._args = await conversion.decode_values(self._args, self._defn.arg_types)
        # Materialize memo + search attributes from the run envelope (a pure
        # function of the immutable input, so re-decoding each replay is safe).
        self._memo, self._typed_sa = await _attributes.decode_attributes(
            self._meta.attributes
        )
        # Decode the run's headers to Payloads (ExecuteWorkflowInput.headers) and
        # build the interceptor chains before any workflow code runs.
        self._headers = await conversion.decode_headers(self._meta.headers)
        self._build_interceptor_chains()
        self._instantiate()
        try:
            while True:
                await self._drain_outside_task()
                await self._sweep_cancellations()
                made_progress = await self._process_commands()
                if made_progress:
                    continue  # immediate timer fires need another drain
                if self._outcome is not None and self._outcome[0] == "task_failure":
                    # Discard uncommitted replies; the retry will regenerate
                    # them identically.
                    raise WorkflowTaskFailure(self._outcome[1])
                await self._flush_outbox()
                if self._outcome is not None:
                    # A rehydrate (query-on-closed) scratch run keeps serving
                    # queries against its reconstructed state after the run
                    # method has completed, until the client signals it is done
                    # (or a serve deadline elapses). Every other run closes.
                    if self._rehydrate_guard() is None or self._rehydrate_stop:
                        break
                    if self._rehydrate_deadline_passed():
                        break
                self._ensure_inbox_waiter()
                # Let newly created tasks claim their function_ids in
                # creation order before the checkpointed race (see module
                # docstring, determinism rule 2).
                await asyncio.sleep(0)
                done, _ = await DBOS.asyncio_wait(
                    [w.task for w in self._waiters],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                await self._deliver(done)
        finally:
            # A replay scratch run (verify OR rehydrate) is a read-only
            # reconstruction of an already-closed workflow: it must NOT emit the
            # terminal-close side effects below (cancelling the source run's
            # in-flight activities, applying ParentClosePolicy to its children,
            # continue-as-new enqueue, failing its abandoned updates, async-gone
            # events) — those would act on the real chain's runs. Only the
            # interpreter's own waiter teardown should run.
            is_replay = _replay.current_guard_for(self._workflow_id) is not None
            if (
                not is_replay
                and self._outcome is not None
                and self._outcome[0] != "task_failure"
            ):
                # Cancel in-flight activities at a terminal close (including
                # continue-as-new), so a fire-and-forget or still-running
                # activity does not outlive the workflow. Local attempts are
                # marked BEFORE tearing down their waiter tasks below (task
                # cancellation unregisters the attempt's live context, after
                # which a still-running threaded function could no longer be
                # reached and would spin forever); a queued activity runs on
                # another worker, so it gets the cross-process cancel signal
                # instead — otherwise its `__temporal_activity` workflow would
                # keep running to completion, orphaned.
                for exec_state in self._pending_activities.values():
                    if exec_state.queued_dbos_id is not None:
                        await self._signal_queued_activity_cancel(exec_state)
                    else:
                        activity_api._request_cancel(
                            (self._workflow_id, exec_state.seq)
                        )
                # Terminal outcome (not a retryable task failure): apply
                # ParentClosePolicy to still-running children.
                await self._sweep_children_on_close()
            if (
                not is_replay
                and self._outcome is not None
                and self._outcome[0] == "continue_as_new"
            ):
                # Enqueue the next run BEFORE tearing down waiters: it must
                # exist before carryover messages can be forwarded to it
                # (FK), and the earlier it exists the sooner senders resolve
                # the chain to it.
                self._can_new_run_id = await self._begin_continue_as_new(
                    self._outcome[1]
                )
            for waiter in self._waiters:
                waiter.task.cancel()
            if self._waiters:
                await asyncio.gather(
                    *(w.task for w in self._waiters), return_exceptions=True
                )
            self._waiters.clear()
            if self._can_new_run_id is not None:
                await self._forward_inbox_to(self._can_new_run_id)
            if (
                not is_replay
                and self._outcome is not None
                and self._outcome[0] != "task_failure"
            ):
                await self._fail_abandoned_updates()
                for exec_state in self._pending_activities.values():
                    if exec_state.async_pending:
                        await DBOS.set_event_async(
                            inbox.async_activity_gone_key(exec_state.activity_id),
                            True,
                        )
                    # (The cancel request itself was delivered before the
                    # waiter teardown above.) Drop cross-attempt state.
                    activity_api._forget_attempt_state(
                        (self._workflow_id, exec_state.seq)
                    )

        self._warn_if_unfinished_handlers()
        kind, value = self._outcome
        if kind == "ok":
            # During a verification replay, a clean completion that consumed
            # fewer steps than were recorded means the replayed code finished
            # early (e.g. an activity at the tail was removed) — divergence DBOS
            # cannot see on its own (no step mismatch ever occurred).
            guard = _replay.current_guard_for(self._workflow_id)
            if (
                guard is not None
                and guard.mode == "verify"
                and self.runtime_history_length() < guard.horizon
            ):
                raise NondeterminismError(
                    "workflow completed before consuming all recorded history "
                    f"(workflow type {self._defn.name!r}); the replayed code "
                    "diverged from the recorded execution"
                )
            return value
        if kind == "cancelled":
            raise WorkflowCancelled(value)
        if kind == "continue_as_new":
            assert self._can_new_run_id is not None
            raise WorkflowContinuedAsNew(self._can_new_run_id)
        assert kind == "failure"
        raise value  # a FailureError; recorded by DBOS as the workflow error

    async def _begin_continue_as_new(self, can: Any) -> str:
        """Enqueue the chain's next run. The run id is deterministic
        (current index + 1) and DBOS records in-workflow starts, so a crash
        anywhere between here and this run's completion replays into an
        idempotent re-attach, never a twin run.
        """
        from . import enqueue, registry
        from .payloads import serialize_retry_policy

        type_name = can._tdb_workflow or self._defn.name
        dispatch_fn = registry.dbos_workflow_for(type_name)
        base, index = ids.parse_run(self._workflow_id)
        new_run_id = ids.run_dbos_id(base, index + 1)
        await self._resolve_own_queue()
        queue_name = can._tdb_task_queue or self._own_queue_name
        queue = (
            await DBOS.retrieve_queue_async(queue_name)
            if queue_name is not None
            else None
        )
        # Run configuration (cron membership, retry policy, run timeout,
        # last-completion carry) follows the chain across a continue-as-new;
        # the attempt counter resets (a CAN run is a fresh execution, as in
        # Temporal). continue_as_new's own run_timeout/retry_policy
        # arguments override the carried values for the new run.
        carried = self._meta.carried_forward()
        # Headers do NOT auto-carry across continue-as-new (Temporal semantics):
        # the outbound chain sets them explicitly (an interceptor re-injects
        # context). _tdb_headers holds raw Payloads; codec-encode here (real
        # loop) into wire form for the next run.
        carried.headers = (
            await conversion.encode_headers(getattr(can, "_tdb_headers", None)) or None
        )
        if can._tdb_run_timeout is not None:
            carried.run_timeout = can._tdb_run_timeout.total_seconds()
        if can._tdb_retry_policy is not None:
            carried.retry_policy = serialize_retry_policy(can._tdb_retry_policy)
        # Memo + search attributes carry forward at their CURRENT (post-upsert)
        # values; continue_as_new's own memo/search_attributes override them
        # for the new run (matching Temporal). Re-encoded only when overridden;
        # otherwise the current encoded form is reused as-is.
        if can._tdb_memo is None and can._tdb_search_attributes is None:
            carried.attributes = await _attributes.encode_attributes(
                self._memo or None, self._typed_sa
            )
        else:
            carried.attributes = await _attributes.encode_attributes(
                can._tdb_memo if can._tdb_memo is not None else (self._memo or None),
                (
                    can._tdb_search_attributes
                    if can._tdb_search_attributes is not None
                    else self._typed_sa
                ),
            )
        # The new run's args come from user code, so encode them (the next
        # run's interpreter decodes against its run signature).
        payload = wrap_input(await conversion.encode_values(can._tdb_args), carried)
        await enqueue.enqueue_run(
            dispatch_fn,
            payload,
            run_id=new_run_id,
            queue=queue,
            run_timeout=carried.run_timeout,
            attributes=carried.attributes,
        )
        return new_run_id

    async def _forward_inbox_to(self, new_run_id: str) -> None:
        """Carryover (Temporal: undelivered signals follow a CAN to the new
        run): forward buffered-undeliverable signals, an outstanding cancel
        request, and any not-yet-consumed inbox messages. Every recv(0) and
        send here is checkpointed, so the forwarding replays identically.
        """
        for envelopes in self._buffered_signals.values():
            for envelope in envelopes:
                await DBOS.send_async(new_run_id, envelope, inbox.INBOX_TOPIC)
        self._buffered_signals.clear()
        if self._cancel_requested:
            await DBOS.send_async(
                new_run_id,
                inbox.cancel_envelope(self._cancel_reason or ""),
                inbox.INBOX_TOPIC,
            )
        while True:
            message = await DBOS.recv_async(inbox.INBOX_TOPIC, 0)
            if message is None:
                return
            if isinstance(message, dict) and message.get("kind") in (
                "activity_result",
                "activity_heartbeat",
            ):
                # Async-activity completions are addressed to THIS run's
                # activities; the new run numbers its own activities from
                # scratch, so forwarding could resolve an unrelated
                # same-id activity with a stale result. The parked
                # activities die with this run (gone-events tell the
                # completer); their late envelopes die here too.
                continue
            await DBOS.send_async(new_run_id, message, inbox.INBOX_TOPIC)

    # ------------------------------------------------------------------
    # Virtual loop driving
    # ------------------------------------------------------------------

    def _instantiate(self) -> None:
        if self._defn.init_takes_args:
            self._instance = self._defn.cls(*self._args)
        else:
            self._instance = self._defn.cls()
        coro = self._run_primary()
        self._primary_task = asyncio.Task(coro, loop=self._vloop)
        self._tasks.add(self._primary_task)

    async def _run_primary(self) -> None:
        try:
            assert self._inbound is not None
            input = _wfi.ExecuteWorkflowInput(
                type=self._defn.cls,
                run_fn=self._defn.run_fn,
                args=self._args,
                headers=self._headers,
            )
            result = await self._inbound.execute_workflow(input)
            self._set_outcome(("ok", result))
        except ContinueAsNewError as can:
            self._set_outcome(("continue_as_new", can))
        except asyncio.CancelledError:
            cancelled = exceptions.CancelledError("Workflow cancelled")
            if self._cancel_requested:
                self._set_outcome(("cancelled", cancelled))
            else:
                # Cancellation nobody requested (e.g. user code cancelling
                # its own task): a workflow failure with a cancelled cause.
                self._set_outcome(("failure", cancelled))
        except BaseException as err:  # noqa: BLE001 — classified below
            self._record_workflow_error(err)

    def _record_workflow_error(self, err: BaseException) -> None:
        # During a verification replay, a divergence DBOS detected mid-run
        # (a different step at a recorded function_id) is terminal — recording
        # it as a task failure would send the dispatcher into its retry loop
        # and mask the divergence. Surface it as a workflow failure the engine
        # recognizes (the dispatcher stamps the nondeterminism marker).
        if isinstance(err, (DBOSUnexpectedStepError, NondeterminismError)) and (
            _replay.current_guard_for(self._workflow_id) is not None
        ):
            self._set_outcome(("failure", err))
            return
        if self._cancel_requested and exceptions.is_cancelled_exception(err):
            self._set_outcome(("cancelled", err))
        elif self._is_failure_exception(err):
            self._set_outcome(("failure", err))
        else:
            self._set_outcome(("task_failure", err))

    def _set_outcome(self, outcome: Tuple[str, Any]) -> None:
        # First outcome wins; a later handler crash can't overwrite a result.
        if self._outcome is None:
            self._outcome = outcome

    def _is_failure_exception(self, err: BaseException) -> bool:
        from . import registry

        return (
            isinstance(err, exceptions.FailureError)
            or isinstance(err, asyncio.TimeoutError)
            or isinstance(err, self._defn.failure_exception_types)
            or isinstance(err, registry.worker_failure_exception_types)
        )

    async def _drain_outside_task(self) -> None:
        """Run ``_drain`` from a plain real-loop callback, not from inside
        the dispatcher task.

        Since Python 3.14, asyncio tracks the currently-executing task
        per-thread rather than per-loop, so entering a virtual-loop task
        while the dispatcher task is mid-step raises "Cannot enter into
        task ... while another task ... is being executed". A ``call_soon``
        callback runs after the dispatcher task has suspended (awaiting
        ``done``), i.e. with no current task on the thread — on every
        Python version.
        """
        loop = asyncio.get_running_loop()
        done: "asyncio.Future[None]" = loop.create_future()

        def run() -> None:
            try:
                self._drain()
            except BaseException as err:  # pragma: no cover — _drain classifies
                done.set_exception(err)
            else:
                done.set_result(None)

        loop.call_soon(run)
        await done

    def _drain(self) -> None:
        """Run the virtual loop until every coroutine is parked: drain ready
        callbacks, then re-evaluate wait_condition predicates, repeating
        while either makes progress (the structure of temporalio's
        ``_run_once``).
        """
        # While draining, workflow coroutines must see the virtual loop as
        # "the" running loop; restore the real loop after (the dispatcher
        # coroutine keeps running on it).
        previous_loop = asyncio._get_running_loop()
        asyncio._set_running_loop(self._vloop)
        try:
            ready = self._vloop.ready
            while ready:
                while ready:
                    handle = ready.popleft()
                    if not handle.cancelled():
                        handle._run()
                still: List[Tuple[Callable[[], bool], "asyncio.Future[Any]"]] = []
                for fn, fut in self._conditions:
                    if fut.done():
                        continue
                    try:
                        satisfied = fn()
                    except BaseException as err:  # noqa: BLE001
                        raise _AbortDrain(err) from err
                    if satisfied:
                        fut.set_result(True)
                    else:
                        still.append((fn, fut))
                self._conditions = still
        except _AbortDrain as abort:
            self._record_workflow_error(abort.cause)
        finally:
            asyncio._set_running_loop(previous_loop)

    # ------------------------------------------------------------------
    # Commands: virtual loop -> real waiters
    # ------------------------------------------------------------------

    def _next_seq(self, kind: str) -> int:
        seq = self._seqs.get(kind, 0) + 1
        self._seqs[kind] = seq
        return seq

    def _assert_not_read_only(self, what: str) -> None:
        if self._read_only:
            raise ReadOnlyContextError(
                f"Cannot {what} in a read-only context (query or update validator)"
            )

    def _create_timer(
        self,
        delay: float,
        callback: Callable[..., Any],
        args: Sequence[Any],
        context: Any,
    ) -> _TimerHandle:
        self._assert_not_read_only("start a timer")
        if delay < 0:
            raise ValueError("Timer delay cannot be negative")
        seq = self._next_seq("timer")
        handle = _TimerHandle(
            seq, self._vloop.time() + delay, callback, args, self._vloop, context
        )
        self._pending_timers[seq] = handle
        self._commands.append(("timer", seq))
        return handle

    def _cancel_timer(self, handle: _TimerHandle) -> None:
        if self._pending_timers.pop(handle.seq, None) is None:
            return
        # If its real waiter exists, retire it; both the creation and this
        # cancellation happen at deterministic points, so replay retires the
        # same waiter.
        for waiter in list(self._waiters):
            if waiter.kind == "timer" and waiter.seq == handle.seq:
                waiter.task.cancel()
                self._waiters.remove(waiter)

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
        result_type: Optional[type] = None,
        task_queue: Optional[str] = None,
        schedule_to_start_timeout: Optional[timedelta] = None,
        headers: Optional[Mapping[str, Any]] = None,
    ) -> ActivityHandle:
        self._assert_not_read_only("start an activity")
        if task_queue is None:
            # Local path: the step must already be registered with this worker.
            # The queued path (task_queue set) targets another worker, where
            # the step lives — so don't require it here.
            activities_mod.attempt_step_for(activity_name)  # raise early if unknown
        seq = self._next_seq("activity")
        resolved_activity_id = activity_id or f"{seq}"
        if any(
            existing.activity_id == resolved_activity_id
            for existing in self._pending_activities.values()
        ):
            # Temporal's server rejects duplicate open activity ids (which
            # would also cross-wire our id-keyed async completion routing);
            # like a rejected command, this fails the workflow task.
            raise ValueError(
                f"Activity id {resolved_activity_id!r} is already in use by "
                "an open activity"
            )
        policy = retry_policy if retry_policy is not None else RetryPolicy()
        policy._validate()
        exec_state = _ActivityExec(
            seq=seq,
            activity_name=activity_name,
            args=list(args),
            retry_policy=policy,
            start_to_close=(
                start_to_close_timeout.total_seconds()
                if start_to_close_timeout
                else None
            ),
            schedule_to_close=(
                schedule_to_close_timeout.total_seconds()
                if schedule_to_close_timeout
                else None
            ),
            activity_id=resolved_activity_id,
            scheduled_at=self._vloop.time(),
            future=self._vloop.create_future(),
            cancellation_type=cancellation_type,
            heartbeat_timeout=(
                heartbeat_timeout.total_seconds() if heartbeat_timeout else None
            ),
            result_type=result_type,
            task_queue=task_queue,
            schedule_to_start=(
                schedule_to_start_timeout.total_seconds()
                if schedule_to_start_timeout
                else None
            ),
            headers=dict(headers or {}),
        )
        self._pending_activities[seq] = exec_state
        self._commands.append(("activity", seq))
        # If the awaiting coroutine is cancelled (workflow cancel, wait_for
        # timeout, explicit handle cancel), the future enters cancelled state
        # during a drain; queue the seq for the deterministic real-side sweep.
        exec_state.future.add_done_callback(
            lambda fut: (
                self._cancelled_activity_seqs.append(seq)
                if fut.cancelled() and seq in self._pending_activities
                else None
            )
        )
        return ActivityHandle(
            exec_state.future,
            on_cancel=lambda: self._request_activity_cancel(seq),
        )

    def _request_activity_cancel(self, seq: int) -> None:
        """Explicit handle.cancel(): WAIT_CANCELLATION_COMPLETED requests
        cancellation and lets the in-flight attempt unwind (the await
        resolves on its confirmation); other types cancel the future, which
        routes through the deterministic sweep."""
        exec_state = self._pending_activities.get(seq)
        if exec_state is None:
            return
        if exec_state.cancellation_type == 1 and not exec_state.async_pending:
            # WAIT_CANCELLATION_COMPLETED: confirmed by the in-flight attempt's
            # unwind — keep the exec open; the awaiter resolves on confirmation.
            # (A parked async activity has no attempt to confirm; it degrades to
            # TRY_CANCEL below and the completer learns via the gone-event.)
            exec_state.cancel_requested = True
            if exec_state.queued_dbos_id is not None:
                # Cross-process: set the cancel event in the async sweep, then
                # the result step delivers the cancelled envelope to confirm.
                self._queued_wait_cancel_seqs.append(seq)
            else:
                activity_api._request_cancel((self._workflow_id, seq))
            return
        exec_state.future.cancel()

    def _rehydrate_guard(self) -> Optional["_replay._ReplayGuard"]:
        """The active guard for this run iff it is a rehydrate (query-on-closed)
        replay — else None."""
        guard = _replay.current_guard_for(self._workflow_id)
        return guard if guard is not None and guard.mode == "rehydrate" else None

    def _rehydrate_deadline_passed(self) -> bool:
        """Whether the rehydrate serve window has elapsed. A fallback that lets
        the scratch run complete if the client never sends a stop signal (e.g.
        it crashed); the normal path stops on the client's stop message."""
        now = asyncio.get_running_loop().time()
        if self._rehydrate_deadline is None:
            self._rehydrate_deadline = now + _replay.REHYDRATE_SERVE_SECONDS
            return False
        return now >= self._rehydrate_deadline

    def _check_replay_horizon(self) -> None:
        """During any replay (verify or rehydrate), refuse to launch a *new*
        durable operation beyond the recorded checkpoint horizon: that means the
        replayed code produced commands not in the recorded history (and would
        run a real activity in the fork). function_ids are 1-based and
        contiguous, so the workflow context's ``function_id`` equals the count
        of steps claimed so far; if it has already reached the horizon, the next
        claim would exceed it. A faithful rehydrate never reaches this (it only
        replays recorded steps, then serves queries), so the guard is mode-
        agnostic."""
        guard = _replay.current_guard_for(self._workflow_id)
        if guard is None:
            return
        ctx = get_local_dbos_context()
        if ctx is not None and ctx.function_id >= guard.horizon:
            raise NondeterminismError(
                "workflow produced new commands beyond its recorded history "
                f"(workflow type {self._defn.name!r}); the replayed code diverged "
                "from the recorded execution"
            )

    async def _process_commands(self) -> bool:
        """Turn queued commands into real-loop waiter tasks. Returns True if
        any virtual-loop progress was made without needing a checkpointed
        wait (expired timers firing immediately, child starts resolving).
        """
        commands, self._commands = self._commands, []
        progressed = False
        for kind, seq in commands:
            if kind == "timer":
                handle = self._pending_timers.get(seq)
                if handle is None:
                    continue  # created and cancelled within one drain
                real_delay = handle.when() - self._vloop.time()
                if real_delay <= 0:
                    # Already due in virtual time: fire deterministically
                    # without a durable sleep (e.g. sleep(0) loops). No
                    # function_id is claimed, so it is replay-horizon-neutral.
                    del self._pending_timers[seq]
                    self._vloop.ready.append(handle)
                    progressed = True
                else:
                    self._check_replay_horizon()
                    self._launch_waiter("timer", seq, DBOS.sleep_async(real_delay))
            elif kind == "activity":
                exec_state = self._pending_activities.get(seq)
                if exec_state is None:
                    # Cancelled within the same drain that started it, before
                    # this dispatch ran: the cancellation sweep already retired
                    # the exec (TRY_CANCEL cancelled the future, so the awaiter
                    # saw CancelledError) and nothing was dispatched on either
                    # path — so there is nothing to launch or cancel.
                    continue
                # Encode the args + headers once (reused across retries); the
                # step decodes them. Done here on the real loop so a codec's
                # async work stays off the virtual loop (headers held raw Payloads
                # from the sync outbound root until now).
                exec_state.args = await conversion.encode_values(exec_state.args)
                exec_state.headers = await conversion.encode_headers(exec_state.headers)
                # Resolve our own queue only when a task_queue was requested,
                # so workflows that never use cross-queue dispatch keep their
                # exact checkpoint shape (no extra status read).
                if exec_state.task_queue is not None:
                    await self._resolve_own_queue()
                self._check_replay_horizon()
                if (
                    exec_state.task_queue is None
                    or exec_state.task_queue == self._own_queue_name
                ):
                    # Local / same-queue path: run as an in-process step.
                    self._launch_attempt(exec_state)
                else:
                    # Cross-queue path (§6.1.2): enqueue on another worker.
                    await self._launch_queued_activity(exec_state)
            elif kind == "child":
                self._check_replay_horizon()
                await self._start_child(self._pending_children[seq])
                progressed = True  # the start future resolved either way
            elif kind == "send":
                self._check_replay_horizon()
                target, envelope, future, resolve_chain = self._pending_sends.pop(seq)
                if isinstance(envelope, dict) and envelope.get("kind") == "signal":
                    # Encode the signal args + headers here (real loop), not in
                    # workflow code, so a codec's async work stays off the
                    # virtual loop (the outbound root left both raw).
                    envelope = {
                        **envelope,
                        "args": await conversion.encode_values(
                            envelope.get("args", [])
                        ),
                        "headers": await conversion.encode_headers(
                            envelope.get("headers")
                        ),
                    }
                # send_async is checkpointed; awaited inline so its
                # function_id claim stays at a deterministic position. The
                # chain resolution is a live read, but that's safe: on
                # replay send_async returns its recorded checkpoint without
                # consuming the resolved value.
                try:
                    if resolve_chain:
                        target = await self._resolve_current_run(target)
                    await DBOS.send_async(target, envelope, inbox.INBOX_TOPIC)
                except Exception as err:  # noqa: BLE001
                    if not future.cancelled():
                        future.set_exception(
                            exceptions.ApplicationError(
                                str(err), type=type(err).__name__
                            )
                        )
                else:
                    if not future.cancelled():
                        future.set_result(None)
                progressed = True
            elif kind == "attributes":
                # An upsert_memo / upsert_search_attributes durably wrote the
                # current attribute state. Encoding (codec) happens here on the
                # real loop, not in the user's sync upsert call. The write is a
                # checkpointed DBOS step claimed at a deterministic position
                # (command order), so it runs once and replays from its
                # recorded result — same model as the "send" branch above.
                encoded = await _attributes.encode_attributes(
                    self._memo or None, self._typed_sa
                )
                self._check_replay_horizon()
                await DBOS.update_workflow_attributes_async(self._workflow_id, encoded)
            elif kind == "patch":
                # A patched()/deprecate_patch() call took the newer path: persist
                # its marker durably so future replays rediscover it. Awaited
                # inline (like "send"/"attributes") so the marker step claims its
                # function_id at this deterministic position; replay reads the id
                # back without re-running the body. A replay scratch run that
                # tries to write a marker past the horizon means the replayed code
                # diverged (it patched where the recording didn't).
                patch_id = self._pending_patches.pop(seq)
                self._check_replay_horizon()
                await _patch_marker(patch_id)
        return progressed

    async def _start_child(self, child: _ChildExec) -> None:
        """Make the child start durable: enqueue its per-type dispatcher
        under the deterministic child id. DBOS records in-workflow starts
        (and SetWorkflowID re-attaches idempotently), so replay re-attaches
        to the same child instead of spawning a twin.
        """
        from . import enqueue, registry

        try:
            dispatch_fn = registry.dbos_workflow_for(child.type_name)
            if await _child_id_taken(child.child_id):
                # Temporal raises into the parent when a child id is already
                # in use; SetWorkflowID would otherwise silently attach to
                # the foreign workflow. (Checkpointed check; the usual
                # TOCTOU window between check and start is documented.)
                del self._pending_children[child.seq]
                if not child.start_future.cancelled():
                    child.start_future.set_exception(
                        exceptions.WorkflowAlreadyStartedError(
                            child.child_id, child.type_name
                        )
                    )
                return
            await self._resolve_own_queue()
            queue_name = child.task_queue or self._own_queue_name
            child_queue = None
            if queue_name is not None:
                child_queue = await DBOS.retrieve_queue_async(queue_name)
            # Record intent BEFORE the start commits (claim-then-start): any
            # child that exists is guaranteed to be in the registry, closing
            # the terminate-races-child-start orphan window. The reverse
            # anomaly — a registry entry whose enqueue never happened — is
            # harmless: policy sweeps skip nonexistent workflows, and
            # recovery re-runs the enqueue anyway.
            self._children_registry.append(
                {"id": child.child_id, "policy": child.parent_close_policy}
            )
            await DBOS.set_event_async(
                inbox.CHILDREN_EVENT_KEY, list(self._children_registry)
            )
            # Encode the child's run args (its interpreter decodes against the
            # child run signature), like a client start.
            child_args = await conversion.encode_values(child.args)
            # Memo + search attributes for the child: into the DBOS attributes
            # column (describe()) and the child's run envelope (its info()).
            child_attrs = await _attributes.encode_attributes(
                child.memo, child.search_attributes
            )
            # Encode headers here (real loop) — child.headers held raw Payloads
            # from the sync outbound root, and a codec is async.
            child_headers = await conversion.encode_headers(child.headers)
            # The child's root is our root if we have one, else us (we are the
            # top of the child's tree) — surfaced as the child's info().root.
            child_root = self._meta.root or {
                "workflow_id": ids.parse_run(self._workflow_id)[0],
                "run_id": self._workflow_id,
            }
            # run_timeout / retry_policy ride in the child's RunMeta exactly as a
            # top-level start: the timeout re-applies across the child's own
            # chain (cron/retry/CAN) and the retry policy gates its workflow
            # retries. run_timeout also drives SetWorkflowTimeout on this initial
            # enqueue (mirroring the continue-as-new path).
            child_meta = RunMeta(
                attributes=child_attrs,
                headers=child_headers or None,
                root=child_root,
                run_timeout=child.run_timeout,
                retry_policy=child.retry_policy,
            )
            child_payload = wrap_input(child_args, child_meta)
            await enqueue.enqueue_run(
                dispatch_fn,
                child_payload,
                run_id=child.child_id,
                queue=child_queue,
                run_timeout=child.run_timeout,
                attributes=child_attrs,
            )
        except Exception as err:  # noqa: BLE001
            del self._pending_children[child.seq]
            if not child.start_future.cancelled():
                child.start_future.set_exception(
                    exceptions.ApplicationError(str(err), type=type(err).__name__)
                )
            return
        child.started = True
        if not child.start_future.cancelled():
            child.start_future.set_result(None)
        self._launch_waiter("child", child.seq, self._await_child_result_decoded(child))

    async def _await_child_result_decoded(self, child: _ChildExec) -> Dict[str, Any]:
        """Await the child's outcome, then decode a successful result against
        the child type's run signature (the decode rides outside the recorded
        step, so it replays deterministically from the recorded envelope)."""
        envelope: Dict[str, Any] = await _await_child_result(child.child_id)
        if envelope.get("ok"):
            from . import registry

            try:
                ret_type = registry.lookup_workflow(child.type_name).ret_type
            except KeyError:
                ret_type = None  # child registered on another worker
            envelope = {
                **envelope,
                "result": await conversion.decode_value(envelope["result"], ret_type),
            }
        return envelope

    async def _signal_queued_activity_cancel(self, exec_state: _ActivityExec) -> None:
        """Deliver cancellation to a queued activity on its own worker, covering
        both states it may be in: (1) running an attempt — its step polls the
        cancel event and unwinds the activity; (2) async-parked after
        raise_complete_async — its workflow is waiting on the completion topic,
        so a cancellation marker there wakes it. Whichever applies fires; the
        other signal is harmlessly ignored. Both are checkpointed (replay-safe).
        """
        assert exec_state.queued_dbos_id is not None
        await DBOS.set_event_async(
            inbox.activity_cancel_key(exec_state.activity_id), True
        )
        await DBOS.send_async(
            exec_state.queued_dbos_id,
            inbox.activity_result_envelope(exec_state.activity_id, cancelled=True),
            inbox.ASYNC_COMPLETE_TOPIC,
        )

    async def _sweep_cancellations(self) -> None:
        """Retire activities and children whose virtual-loop futures were
        cancelled during the drain. Cancellation decisions are deterministic
        workflow state, so this sweep replays identically.

        Activities (local path): TRY_CANCEL (default) cancels the real step task
        (which records nothing — replays like a crash); ABANDON detaches it; WAIT
        is handled in ``_request_activity_cancel`` (kept open until confirmed).

        Activities (queued path, §6.1.2): the activity runs on another worker, so
        cancellation is delivered cross-process by ``_signal_queued_activity_cancel``
        (a cancel event the running attempt polls, plus a marker that wakes an
        async-parked activity). ABANDON leaves it running. WAIT keeps the exec open until
        the activity confirms via the result step (handled separately below).

        Children: non-ABANDON types deliver the child's cooperative-cancel
        envelope (a checkpointed send) and retire the waiter; ABANDON just
        retires the waiter. The WAIT variants are approximated (the awaiter
        is already gone once the future is cancelled).
        """
        # WAIT_CANCELLATION_COMPLETED on the queued path: set the cancel event
        # but keep the exec/waiter open — the result step delivers the activity's
        # cancelled envelope to confirm the unwind (see _deliver_queued_activity_event).
        wait_seqs, self._queued_wait_cancel_seqs = self._queued_wait_cancel_seqs, []
        for seq in wait_seqs:
            exec_state = self._pending_activities.get(seq)
            if exec_state is not None and exec_state.queued_dbos_id is not None:
                await self._signal_queued_activity_cancel(exec_state)

        seqs, self._cancelled_activity_seqs = self._cancelled_activity_seqs, []
        for seq in seqs:
            exec_state = self._pending_activities.pop(seq, None)
            if exec_state is None:
                continue
            if exec_state.cancellation_type != 2:  # ABANDON never requests
                if exec_state.queued_dbos_id is not None:
                    await self._signal_queued_activity_cancel(exec_state)
                else:
                    # Local: mark the (possibly threaded, still-running) attempt
                    # so it observes cancellation at its next heartbeat.
                    activity_api._request_cancel((self._workflow_id, seq))
            activity_api._forget_attempt_state((self._workflow_id, seq))
            if exec_state.async_pending:
                # Tell the external completer (checkpointed event): its next
                # heartbeat/complete raises instead of vanishing.
                await DBOS.set_event_async(
                    inbox.async_activity_gone_key(exec_state.activity_id), True
                )
            for waiter in list(self._waiters):
                if waiter.kind in ("activity", "act_s2c", "act_hb", "q_activity") and (
                    waiter.seq == seq
                ):
                    self._waiters.remove(waiter)
                    if exec_state.cancellation_type == 2:  # ABANDON
                        self._abandoned_tasks.add(waiter.task)
                        waiter.task.add_done_callback(self._discard_abandoned)
                    else:
                        waiter.task.cancel()

        child_seqs, self._cancelled_child_seqs = self._cancelled_child_seqs, []
        for seq in child_seqs:
            child = self._pending_children.pop(seq, None)
            if child is None:
                continue
            for waiter in list(self._waiters):
                if waiter.kind == "child" and waiter.seq == seq:
                    self._waiters.remove(waiter)
                    waiter.task.cancel()
            if child.cancellation_type != 0 and child.started:  # not ABANDON
                # The cancel must reach the child's *current* run (it may
                # have continued as new since we started it).
                current = await self._resolve_current_run(child.child_id)
                await DBOS.send_async(
                    current, inbox.cancel_envelope(), inbox.INBOX_TOPIC
                )

    def _discard_abandoned(self, task: "asyncio.Task[Any]") -> None:
        self._abandoned_tasks.discard(task)
        if not task.cancelled():
            task.exception()  # retrieve, suppressing the unretrieved warning

    def _launch_waiter(self, kind: str, seq: int, coro: Any) -> None:
        task = asyncio.ensure_future(coro)
        self._waiters.append(_Waiter(kind=kind, seq=seq, task=task))

    def _launch_attempt(self, exec_state: _ActivityExec) -> None:
        step_fn = activities_mod.attempt_step_for(exec_state.activity_name)
        exec_state.in_backoff = False
        meta = {
            "activity_id": exec_state.activity_id,
            "activity_type": exec_state.activity_name,
            "attempt": exec_state.attempt,
            "heartbeat_timeout": exec_state.heartbeat_timeout,
            "schedule_to_close": exec_state.schedule_to_close,
            "start_to_close": exec_state.start_to_close,
            "retry_policy": serialize_retry_policy(exec_state.retry_policy),
            "seq": exec_state.seq,
            "task_queue": exec_state.task_queue or self._task_queue_name,
            "workflow_id": ids.parse_run(self._workflow_id)[0],
            "workflow_run_id": self._workflow_id,
            "workflow_type": self._defn.name,
            "headers": exec_state.headers,
        }
        # Call step_fn synchronously so its function_id is claimed here (a
        # deterministic position); the result decode rides outside the
        # recorded step, replaying from the recorded envelope.
        step_coro = step_fn(exec_state.args, exec_state.start_to_close, meta)
        self._launch_waiter(
            "activity",
            exec_state.seq,
            self._decode_activity_result(step_coro, exec_state),
        )

    async def _decode_activity_result(
        self, step_coro: Coroutine[Any, Any, Dict[str, Any]], exec_state: _ActivityExec
    ) -> Dict[str, Any]:
        envelope: Dict[str, Any] = await step_coro
        if envelope.get("ok"):
            from . import registry

            ret_type = exec_state.result_type
            if ret_type is None:
                try:
                    ret_type = registry.lookup_activity(
                        exec_state.activity_name
                    ).ret_type
                except KeyError:
                    ret_type = None
            envelope = {
                **envelope,
                "result": await conversion.decode_value(envelope["result"], ret_type),
            }
        return envelope

    async def _launch_queued_activity(self, exec_state: _ActivityExec) -> None:
        """Cross-queue / distributed activity dispatch (§6.1.2): enqueue the
        ``__temporal_activity`` workflow on the target DBOS queue and await its
        result envelope, mirroring the child-workflow enqueue (``_start_child``).

        The activity-workflow id is the deterministic ``{run_id}--a{seq}`` so a
        crash anywhere after the enqueue replays into an idempotent re-attach
        (``SetWorkflowID``), exactly as child ids do — DBOS records the
        in-workflow start, so recovery re-attaches to the same activity instead
        of spawning a twin. ``--a`` is a reserved separator (``ids.ACTIVITY_SEPARATOR``,
        like ``--r``), so the id can never collide with a user/child/run id.
        """
        from dbos import SetWorkflowID

        from . import registry

        # Only reached from _process_commands when task_queue is set and differs
        # from our own queue.
        assert exec_state.task_queue is not None
        seq = exec_state.seq
        activity_dbos_id = ids.activity_dbos_id(self._workflow_id, seq)
        meta = {
            "activity_id": exec_state.activity_id,
            "activity_type": exec_state.activity_name,
            "attempt": exec_state.attempt,
            "heartbeat_timeout": exec_state.heartbeat_timeout,
            "seq": seq,
            # The activity runs on the target worker; report that queue.
            "task_queue": exec_state.task_queue,
            "workflow_id": ids.parse_run(self._workflow_id)[0],
            "workflow_run_id": self._workflow_id,
            "workflow_type": self._defn.name,
            "headers": exec_state.headers,
            # Tells the attempt step to poll the cross-process cancel event.
            "queued": True,
            # The activity workflow id, so info().task_token addresses this
            # workflow for raise_complete_async external completion.
            "queued_activity_dbos_id": activity_dbos_id,
        }
        payload = {
            "activity_name": exec_state.activity_name,
            "args": exec_state.args,  # already encoded in _process_commands
            "start_to_close": exec_state.start_to_close,
            "schedule_to_close": exec_state.schedule_to_close,
            "schedule_to_start": exec_state.schedule_to_start,
            # The activity workflow owns the retry loop (Design A); schedule_to_close
            # gates retries there, not via SetWorkflowTimeout — matching the local
            # path, where it bounds the retry sequence, not an in-flight attempt.
            "retry_policy": serialize_retry_policy(exec_state.retry_policy),
            "meta": meta,
        }
        try:
            dispatch_fn = registry.activity_dispatcher_fn()
            queue = await DBOS.retrieve_queue_async(exec_state.task_queue)
            if queue is None:
                raise RuntimeError(
                    f"Task queue {exec_state.task_queue!r} is not registered "
                    "(no worker has declared it)"
                )
            with SetWorkflowID(activity_dbos_id):
                await queue.enqueue_async(dispatch_fn, payload)
        except Exception as err:  # noqa: BLE001 — surface as an ActivityError
            self._pending_activities.pop(seq, None)
            if not exec_state.future.cancelled():
                error = exceptions.ActivityError(
                    "Failed to schedule activity on task queue "
                    f"{exec_state.task_queue!r}",
                    scheduled_event_id=0,
                    started_event_id=0,
                    identity="",
                    activity_type=exec_state.activity_name,
                    activity_id=exec_state.activity_id,
                    retry_state=None,
                )
                error.__cause__ = exceptions.ApplicationError(
                    str(err), type=type(err).__name__
                )
                exec_state.future.set_exception(error)
            return
        # Record the dispatched id so cancellation can reach the activity on
        # its own worker (cooperatively — a checkpointed cancel event plus a
        # completion-topic marker; see _signal_queued_activity_cancel — not a
        # native DBOS cancel of this workflow).
        exec_state.queued_dbos_id = activity_dbos_id
        # Claim the result step's function_id at this deterministic position
        # (like _launch_attempt); the decode rides outside the recorded step.
        step_coro = _await_activity_result(activity_dbos_id)
        self._launch_waiter(
            "q_activity", seq, self._decode_activity_result(step_coro, exec_state)
        )
        if exec_state.cancel_requested:
            # A WAIT_CANCELLATION_COMPLETED cancel requested before this dispatch
            # committed: queued_dbos_id was still None when the sweep ran, so the
            # cross-process signal was deferred to here (now that the activity
            # workflow exists and its result waiter is in place to confirm the
            # unwind). TRY_CANCEL doesn't reach here — it cancels the future,
            # retiring the exec before dispatch (handled in _process_commands).
            await self._signal_queued_activity_cancel(exec_state)

    def _ensure_inbox_waiter(self) -> None:
        if not any(w.kind == "inbox" for w in self._waiters):
            recv_timeout = inbox.RECV_TIMEOUT_SECONDS
            if self._rehydrate_guard() is not None:
                # Serving a rehydrate query: poll often so the serve deadline
                # (re-checked at the loop top) stays effective even if the
                # client never sends a stop signal — a full RECV_TIMEOUT would
                # otherwise pin the scratch run alive for an hour.
                recv_timeout = min(recv_timeout, _replay.REHYDRATE_POLL_SECONDS)
            self._launch_waiter(
                "inbox",
                0,
                DBOS.recv_async(inbox.INBOX_TOPIC, recv_timeout),
            )

    async def _flush_outbox(self) -> None:
        # set_event is checkpointed per call: replay re-flushes identically.
        # The outbox holds only update/query reply payloads; encode a present
        # "result" (the client decodes it against the handler's signature).
        outbox, self._outbox = self._outbox, []
        for key, value in outbox:
            if isinstance(value, dict) and "result" in value:
                value = {
                    **value,
                    "result": await conversion.encode_value(value["result"]),
                }
            await DBOS.set_event_async(key, value)

    # ------------------------------------------------------------------
    # Event delivery: checkpointed real-world facts -> virtual loop
    # ------------------------------------------------------------------

    def _advance_time(self, to_seconds: Optional[float]) -> None:
        if to_seconds is not None and to_seconds > self._vloop.time_seconds:
            self._vloop.time_seconds = to_seconds

    async def _deliver(self, done: Set["asyncio.Task[Any]"]) -> None:
        for waiter in list(self._waiters):
            if waiter.task not in done:
                continue
            self._waiters.remove(waiter)
            if waiter.kind == "inbox":
                await self._deliver_inbox(waiter.task.result())
            elif waiter.kind == "timer":
                self._deliver_timer(waiter.seq)
            elif waiter.kind == "activity":
                self._deliver_activity_event(waiter)
            elif waiter.kind == "q_activity":
                self._deliver_queued_activity_event(waiter)
            elif waiter.kind == "act_s2c":
                self._async_parked_timeout(
                    waiter.seq, exceptions.TimeoutType.START_TO_CLOSE
                )
            elif waiter.kind == "act_hb":
                self._async_heartbeat_check(waiter.seq)
            elif waiter.kind == "child":
                self._deliver_child_event(waiter)

    def _deliver_timer(self, seq: int) -> None:
        handle = self._pending_timers.pop(seq, None)
        if handle is None:
            return  # cancelled while its waiter was completing
        self._advance_time(handle.when())
        self._vloop.ready.append(handle)

    def _deliver_activity_event(self, waiter: _Waiter) -> None:
        exec_state = self._pending_activities.get(waiter.seq)
        if exec_state is None or exec_state.future.cancelled():
            # Cancelled between completion and delivery; drop the result.
            self._pending_activities.pop(waiter.seq, None)
            return
        if exec_state.in_backoff:
            # Backoff sleep finished -> next attempt.
            exec_state.attempt += 1
            self._launch_attempt(exec_state)
            return
        envelope: Dict[str, Any] = waiter.task.result()
        self._advance_time(envelope.get("ended_at"))
        if envelope.get("async_pending"):
            # raise_complete_async(): the function returned but the activity
            # stays pending; an activity_result inbox envelope resolves it.
            # The marker is checkpointed, so recovery re-parks identically.
            # Timeouts keep applying while parked (Temporal semantics),
            # via durable-sleep waiters: a one-shot start-to-close (measured
            # from the park, slightly more generous than Temporal's
            # attempt-start) and a re-arming heartbeat-window check.
            exec_state.async_pending = True
            if exec_state.start_to_close is not None:
                elapsed = float(envelope.get("ended_at", 0.0)) - float(
                    envelope.get("started_at", envelope.get("ended_at", 0.0))
                )
                remaining = max(0.05, exec_state.start_to_close - max(elapsed, 0.0))
                self._launch_waiter(
                    "act_s2c",
                    exec_state.seq,
                    DBOS.sleep_async(remaining),
                )
            if exec_state.heartbeat_timeout is not None:
                exec_state.async_hb_seen = False
                self._launch_waiter(
                    "act_hb",
                    exec_state.seq,
                    DBOS.sleep_async(exec_state.heartbeat_timeout),
                )
            return
        if envelope["ok"]:
            del self._pending_activities[waiter.seq]
            activity_api._forget_attempt_state((self._workflow_id, waiter.seq))
            exec_state.future.set_result(envelope["result"])
            return
        failure: FailureEnvelope = envelope["failure"]
        exec_state.last_failure = failure
        if exec_state.cancel_requested and isinstance(
            deserialize_failure(failure), exceptions.CancelledError
        ):
            # WAIT_CANCELLATION_COMPLETED confirmation: the attempt observed
            # the request and unwound; only now does the awaiter see the
            # cancellation.
            del self._pending_activities[waiter.seq]
            activity_api._forget_attempt_state((self._workflow_id, waiter.seq))
            exec_state.future.cancel()
            return
        retry_delay, retry_state = self._retry_decision(exec_state, failure)
        if retry_delay is not None:
            exec_state.in_backoff = True
            self._launch_waiter(
                "activity", exec_state.seq, DBOS.sleep_async(retry_delay)
            )
            return
        del self._pending_activities[waiter.seq]
        activity_api._forget_attempt_state((self._workflow_id, waiter.seq))
        error = exceptions.ActivityError(
            "Activity task failed",
            scheduled_event_id=0,
            started_event_id=0,
            identity="",
            activity_type=exec_state.activity_name,
            activity_id=exec_state.activity_id,
            retry_state=retry_state,
        )
        error.__cause__ = deserialize_failure(failure)
        exec_state.future.set_exception(error)

    def _deliver_queued_activity_event(self, waiter: _Waiter) -> None:
        """Resolve a cross-queue activity from the ``__temporal_activity``
        workflow's result envelope. The activity workflow owns retries/timeouts
        on its own worker (Design A), so this is a terminal delivery — no
        interpreter-side retry on this path, unlike ``_deliver_activity_event``.
        """
        exec_state = self._pending_activities.get(waiter.seq)
        if exec_state is None or exec_state.future.cancelled():
            # Cancelled between completion and delivery; drop the result.
            self._pending_activities.pop(waiter.seq, None)
            return
        envelope: Dict[str, Any] = waiter.task.result()
        self._advance_time(envelope.get("ended_at"))
        del self._pending_activities[waiter.seq]
        # No async_pending here: raise_complete_async() parks inside the
        # __temporal_activity workflow on its own worker (Design A), which only
        # returns once externally completed — so this is always a terminal
        # ok/failure envelope.
        if envelope["ok"]:
            exec_state.future.set_result(envelope["result"])
            return
        failure: FailureEnvelope = envelope["failure"]
        exec_state.last_failure = failure
        if exec_state.cancel_requested and isinstance(
            deserialize_failure(failure), exceptions.CancelledError
        ):
            # WAIT_CANCELLATION_COMPLETED confirmation: the activity observed the
            # cancel event, unwound on its worker, and reported cancelled — only
            # now does the awaiter see the cancellation.
            exec_state.future.cancel()
            return
        # The activity workflow ran the retry loop and stamped the terminal
        # retry_state into the envelope when it gave up.
        retry_state_value = envelope.get("retry_state")
        retry_state = (
            exceptions.RetryState(retry_state_value)
            if retry_state_value is not None
            else None
        )
        error = exceptions.ActivityError(
            "Activity task failed",
            scheduled_event_id=0,
            started_event_id=0,
            identity="",
            activity_type=exec_state.activity_name,
            activity_id=exec_state.activity_id,
            retry_state=retry_state,
        )
        error.__cause__ = deserialize_failure(failure)
        exec_state.future.set_exception(error)

    def _deliver_child_event(self, waiter: _Waiter) -> None:
        child = self._pending_children.pop(waiter.seq, None)
        if child is None or child.result_future.cancelled():
            return  # cancelled between completion and delivery
        envelope: Dict[str, Any] = waiter.task.result()
        self._advance_time(envelope.get("ended_at"))
        if envelope["ok"]:
            child.result_future.set_result(envelope["result"])
            return
        error = exceptions.ChildWorkflowError(
            "Child workflow execution failed",
            namespace="default",
            workflow_id=child.child_id,
            run_id=child.child_id,
            workflow_type=child.type_name,
            initiated_event_id=0,
            started_event_id=0,
            retry_state=None,
        )
        error.__cause__ = deserialize_failure(envelope["failure"])
        child.result_future.set_exception(error)

    async def _sweep_children_on_close(self) -> None:
        """ParentClosePolicy (§6.5): when the parent reaches a terminal
        outcome, deal with still-running children. TERMINATE (the default)
        native-cancels them — recursively, applying *their* recorded
        policies, since a terminated child runs no code of its own;
        REQUEST_CANCEL delivers the cooperative envelope; ABANDON leaves
        them running. Re-running this on replay is idempotent.
        """
        for child in list(self._pending_children.values()):
            if not child.started:
                continue
            policy = child.parent_close_policy
            if policy == 2:  # ABANDON
                continue
            if policy == 3:  # REQUEST_CANCEL
                # The policy applies to the child's *chain*: a child that
                # continued as new lives at a later run now.
                current = await self._resolve_current_run(child.child_id)
                await DBOS.send_async(
                    current, inbox.cancel_envelope(), inbox.INBOX_TOPIC
                )
            else:  # TERMINATE (1) and UNSPECIFIED (0) default to terminate
                await self._terminate_child_tree(child.child_id, set())

    async def _terminate_child_tree(self, child_id: str, visited: Set[str]) -> None:
        """Terminate a child and apply its recorded parent-close policies to
        its own descendants (it runs no code, so nobody else will)."""
        if child_id in visited:
            return
        visited.add(child_id)
        # Operate on the chain's current run: a child that continued as new
        # lives at a later run, and each closed run already swept the
        # children *it* started, so only the current run's registry matters.
        current = await self._resolve_current_run(child_id)
        fields = await _safe_status(current)
        if fields is None or fields["status"] not in ("PENDING", "ENQUEUED", "DELAYED"):
            return  # already terminal (or stuck); don't clobber its status
        await DBOS.cancel_workflow_async(current)
        grandchildren = await DBOS.get_event_async(current, inbox.CHILDREN_EVENT_KEY, 0)
        for grandchild in grandchildren or []:
            policy = grandchild.get("policy", 1)
            if policy == 2:  # ABANDON
                continue
            if policy == 3:  # REQUEST_CANCEL
                await DBOS.send_async(
                    grandchild["id"], inbox.cancel_envelope(), inbox.INBOX_TOPIC
                )
            else:
                await self._terminate_child_tree(grandchild["id"], visited)

    def _retry_decision(
        self, exec_state: _ActivityExec, failure: FailureEnvelope
    ) -> Tuple[Optional[float], exceptions.RetryState]:
        """The local path's retry decision: elapsed comes from virtual time;
        the policy logic itself lives in the shared ``activities.retry_decision``
        (the queued path uses it too)."""
        elapsed = (
            self._vloop.time() - exec_state.scheduled_at
            if exec_state.schedule_to_close is not None
            else None
        )
        return activities_mod.retry_decision(
            exec_state.retry_policy,
            exec_state.attempt,
            failure,
            elapsed=elapsed,
            schedule_to_close=exec_state.schedule_to_close,
        )

    # ------------------------------------------------------------------
    # Inbox routing
    # ------------------------------------------------------------------

    async def _deliver_inbox(self, message: Any) -> None:
        if message is None:
            return  # recv timeout (checkpointed; deterministic); re-armed next round
        if not isinstance(message, dict) or "kind" not in message:
            logger.warning(
                "Workflow %s: dropping malformed inbox message %r",
                self._workflow_id,
                message,
            )
            return
        envelope: inbox.Envelope = message
        self._advance_time(envelope.get("sent_at"))
        kind = envelope["kind"]
        if kind == "signal":
            # Decode happens inside _apply_signal's handler branch, NOT here: a
            # signal with no handler is buffered and may be forwarded across a
            # continue-as-new, so it must keep its *encoded* args for the
            # consuming run to decode against that run's handler signature.
            await self._apply_signal(envelope)
        elif kind == "update":
            # Updates/queries always have a handler (an unknown one is rejected,
            # never buffered/forwarded), so decode against its signature here.
            envelope = await self._decode_message_args(envelope)
            await self._apply_update(envelope)
        elif kind == "query":
            envelope = await self._decode_message_args(envelope)
            await self._apply_query(envelope)
        elif kind == "activity_result":
            await self._apply_activity_result(envelope)
        elif kind == "activity_heartbeat":
            await self._apply_activity_heartbeat(envelope)
        elif kind == "cancel":
            self._apply_cancel(envelope)
        elif kind == "rehydrate_stop":
            # The querying client is done with this rehydrated scratch run; let
            # it stop serving and complete (see the rehydrate serve loop).
            self._rehydrate_stop = True
        else:
            logger.warning(
                "Workflow %s: unknown inbox envelope kind %r", self._workflow_id, kind
            )

    async def _decode_message_args(self, envelope: inbox.Envelope) -> inbox.Envelope:
        kind = envelope["kind"]
        name = envelope["name"]
        defn: Any = None
        if kind == "signal":
            defn = self._defn.signals.get(name) or self._defn.signals.get(None)
        elif kind == "update":
            defn = self._defn.updates.get(name) or self._defn.updates.get(None)
        elif kind == "query":
            defn = self._defn.queries.get(name) or self._defn.queries.get(None)
        if defn is not None and defn.name is None:
            # Dynamic (catch-all) handler: deliver (name, Sequence[RawValue]) —
            # the raw payloads wrapped untouched so the handler converts them
            # itself via workflow.payload_converter().
            raw_args = envelope.get("args", [])
            raw = await conversion.decode_values(raw_args, [RawValue] * len(raw_args))
            return {**envelope, "args": [name, raw]}
        arg_types = defn.arg_types if defn is not None else None
        decoded = await conversion.decode_values(envelope.get("args", []), arg_types)
        return {**envelope, "args": decoded}

    def _async_pending_by_id(self, activity_id: str) -> Optional[_ActivityExec]:
        for exec_state in self._pending_activities.values():
            if exec_state.activity_id == activity_id and exec_state.async_pending:
                return exec_state
        return None

    async def _apply_activity_result(self, envelope: inbox.Envelope) -> None:
        """External completion of an async activity
        (client.get_async_activity_handle). Checkpointed inbox delivery, so
        the resolution replays identically; failures consult the retry
        policy (Temporal semantics — the function re-runs)."""
        activity_id = str(envelope.get("activity_id", ""))
        exec_state = self._async_pending_by_id(activity_id)
        if exec_state is None:
            logger.debug(
                "Workflow %s: dropping activity_result for unknown/non-async "
                "activity id %s",
                self._workflow_id,
                activity_id,
            )
            return
        seq = exec_state.seq
        self._retire_parked_timers(seq)
        if envelope.get("cancelled"):
            del self._pending_activities[seq]
            activity_api._forget_attempt_state((self._workflow_id, seq))
            exec_state.future.cancel()
        elif envelope["ok"]:
            del self._pending_activities[seq]
            activity_api._forget_attempt_state((self._workflow_id, seq))
            from . import registry

            try:
                ret_type = registry.lookup_activity(exec_state.activity_name).ret_type
            except KeyError:
                ret_type = None
            result = await conversion.decode_value(envelope.get("result"), ret_type)
            exec_state.future.set_result(result)
        else:
            # An external fail goes through the retry policy, like Temporal:
            # the next attempt re-runs the activity function (which may park
            # async again, yielding a fresh wait under the same token).
            self._fail_async_attempt(exec_state, envelope["failure"])

    def _fail_async_attempt(
        self, exec_state: _ActivityExec, failure: FailureEnvelope
    ) -> None:
        """Shared failure path for parked async activities (external fail,
        parked start-to-close, parked heartbeat timeout): retry per policy
        (re-running the function) or resolve with ActivityError."""
        seq = exec_state.seq
        exec_state.async_pending = False
        exec_state.last_failure = failure
        retry_delay, retry_state = self._retry_decision(exec_state, failure)
        if retry_delay is not None:
            exec_state.in_backoff = True
            self._launch_waiter("activity", seq, DBOS.sleep_async(retry_delay))
            return
        del self._pending_activities[seq]
        activity_api._forget_attempt_state((self._workflow_id, seq))
        error = exceptions.ActivityError(
            "Activity task failed",
            scheduled_event_id=0,
            started_event_id=0,
            identity="",
            activity_type=exec_state.activity_name,
            activity_id=exec_state.activity_id,
            retry_state=retry_state,
        )
        error.__cause__ = deserialize_failure(failure)
        exec_state.future.set_exception(error)

    def _retire_parked_timers(self, seq: int) -> None:
        for waiter in list(self._waiters):
            if waiter.kind in ("act_s2c", "act_hb") and waiter.seq == seq:
                self._waiters.remove(waiter)
                waiter.task.cancel()
                self._abandoned_tasks.add(waiter.task)
                waiter.task.add_done_callback(self._discard_abandoned)

    def _async_parked_timeout(
        self, seq: int, timeout_type: "exceptions.TimeoutType"
    ) -> None:
        exec_state = self._pending_activities.get(seq)
        if exec_state is None or not exec_state.async_pending:
            return  # resolved (or retired) before the timer fired
        self._retire_parked_timers(seq)
        timeout_failure = exceptions.TimeoutError(
            (
                "activity Start-To-Close timeout"
                if timeout_type == exceptions.TimeoutType.START_TO_CLOSE
                else "activity Heartbeat timeout"
            ),
            type=timeout_type,
            last_heartbeat_details=[],
        )
        from .payloads import serialize_failure

        self._fail_async_attempt(exec_state, serialize_failure(timeout_failure))

    def _async_heartbeat_check(self, seq: int) -> None:
        exec_state = self._pending_activities.get(seq)
        if exec_state is None or not exec_state.async_pending:
            return
        if not exec_state.async_hb_seen:
            self._async_parked_timeout(seq, exceptions.TimeoutType.HEARTBEAT)
            return
        # A heartbeat arrived within the window: re-arm.
        exec_state.async_hb_seen = False
        assert exec_state.heartbeat_timeout is not None
        self._launch_waiter(
            "act_hb", seq, DBOS.sleep_async(exec_state.heartbeat_timeout)
        )

    async def _apply_activity_heartbeat(self, envelope: inbox.Envelope) -> None:
        """Heartbeat for an async-pending activity from the external
        completer; refreshes the parked heartbeat-timeout window, and the
        details surface on the next retry attempt (in-process, like
        in-activity heartbeats)."""
        activity_id = str(envelope.get("activity_id", ""))
        exec_state = self._async_pending_by_id(activity_id)
        if exec_state is not None:
            exec_state.async_hb_seen = True
            activity_api._heartbeat_store[(self._workflow_id, exec_state.seq)] = (
                await conversion.decode_values(envelope.get("details", []))
            )

    def _apply_cancel(self, envelope: inbox.Envelope) -> None:
        """Cooperative cancellation (§6.5): raise CancelledError into the
        primary task at the next event boundary. Cleanup code runs — and may
        still execute activities, because the outer loop keeps servicing
        events until the unwind produces an outcome.
        """
        if self._cancel_requested:
            # Temporal dedups cancel requests server-side: a second cancel
            # must not re-interrupt cleanup code mid-unwind.
            return
        self._cancel_requested = True
        reason = envelope.get("reason")
        self._cancel_reason = str(reason) if reason else None
        if self._primary_task is not None and not self._primary_task.done():
            self._vloop.call_soon(self._primary_task.cancel)

    async def _apply_signal(self, envelope: inbox.Envelope) -> None:
        # Exact match first, then the dynamic (catch-all) handler if one is
        # registered (name key ``None``).
        defn = self._defn.signals.get(envelope["name"]) or self._defn.signals.get(None)
        if defn is None:
            # Buffered with its *encoded* args for delivery if a handler is
            # registered later (dynamic registration arrives in Phase 2) or, more
            # commonly, for forwarding across a continue-as-new. Decoding here
            # would corrupt that forward path (the next run re-decodes against
            # its own handler signature, and the JSON serializer would choke on
            # raw user values at the send checkpoint).
            self._buffered_signals.setdefault(envelope["name"], []).append(envelope)
            logger.debug(
                "Workflow %s: buffering signal %r with no handler",
                self._workflow_id,
                envelope["name"],
            )
            return
        decoded = await self._decode_message_args(envelope)
        # Decode headers here (real loop) so the codec runs off the virtual loop;
        # the handler task gets ready Payloads.
        headers = await conversion.decode_headers(envelope.get("headers"))
        self._spawn_handler(
            # Pass the incoming signal name (not defn.name, which is None for a
            # dynamic handler): the inbound chain re-resolves it, and an
            # interceptor sees the real name.
            self._run_signal_handler(envelope["name"], decoded["args"], headers),
            kind="signal",
            # A dynamic handler has no name of its own; label it by the
            # incoming signal name (for unfinished-handler warnings).
            name=defn.name or envelope["name"],
            policy=defn.unfinished_policy,
        )

    async def _run_signal_handler(
        self, name: str, args: Sequence[Any], headers: Mapping[str, Any]
    ) -> None:
        try:
            assert self._inbound is not None
            await self._inbound.handle_signal(
                _wfi.HandleSignalInput(signal=name, args=args, headers=headers)
            )
        except ContinueAsNewError as can:
            # Handlers may initiate continue-as-new (as in Temporal).
            self._set_outcome(("continue_as_new", can))
        except BaseException as err:  # noqa: BLE001
            # Same classification as the primary coroutine (Temporal
            # semantics: a failure exception in a signal handler fails the
            # workflow; anything else fails the workflow task).
            self._record_workflow_error(err)

    def _spawn_handler(
        self,
        coro: Any,
        *,
        kind: str,
        name: str,
        policy: int,
        handler_id: Optional[str] = None,
    ) -> None:
        task: asyncio.Task[Any] = asyncio.Task(coro, loop=self._vloop)
        self._inflight_handlers[task] = {
            "kind": kind,
            "name": name,
            "id": handler_id,
            "policy": policy,
        }
        self._tasks.add(task)

        def _finished(t: "asyncio.Task[Any]") -> None:
            self._inflight_handlers.pop(t, None)
            self._tasks.discard(t)

        task.add_done_callback(_finished)

    async def _fail_abandoned_updates(self) -> None:
        """Accepted updates whose handlers the closing run abandoned get a
        failure reply (Temporal fails them with AcceptedUpdateCompletedWorkflow
        when the workflow completes); without this the caller would block
        until its timeout. Checkpointed set_events in deterministic
        (spawn) order, so replay re-emits identically.
        """
        from .payloads import serialize_failure

        for record in self._inflight_handlers.values():
            if record["kind"] != "update" or record["id"] is None:
                continue
            failure = exceptions.ApplicationError(
                "Workflow run finished before the update handler completed",
                type="AcceptedUpdateCompletedWorkflow",
                non_retryable=True,
            )
            await DBOS.set_event_async(
                inbox.update_result_key(record["id"]),
                {"status": "failed", "failure": serialize_failure(failure)},
            )

    def _warn_if_unfinished_handlers(self) -> None:
        """Temporal's HandlerUnfinishedPolicy behavior: a workflow that
        reaches a terminal outcome while handlers are mid-flight abandons
        them, and warns for each handler whose policy is WARN_AND_ABANDON.
        Guard with `await workflow.wait_condition(lambda:
        workflow.all_handlers_finished())` before returning.
        """
        warnable = [
            record
            for record in self._inflight_handlers.values()
            if record["policy"] == HandlerUnfinishedPolicy.WARN_AND_ABANDON.value
        ]
        updates = [r for r in warnable if r["kind"] == "update"]
        if updates:
            warnings.warn(
                UnfinishedUpdateHandlersWarning(
                    _unfinished_handler_message(
                        "update",
                        "the client that sent "
                        "the update will never receive its result",
                    )
                    + json.dumps([{"name": r["name"], "id": r["id"]} for r in updates])
                )
            )
        signals = [r for r in warnable if r["kind"] == "signal"]
        if signals:
            counts = Counter(r["name"] for r in signals)
            warnings.warn(
                UnfinishedSignalHandlersWarning(
                    _unfinished_handler_message("signal", "its work was interrupted")
                    + json.dumps(
                        [{"name": n, "count": c} for n, c in counts.most_common()]
                    )
                )
            )

    async def _apply_update(self, envelope: inbox.Envelope) -> None:
        update_id: str = envelope["update_id"]
        if update_id in self._seen_update_ids:
            return  # duplicate delivery; the original reply event stands
        self._seen_update_ids.add(update_id)
        reply_key = inbox.update_result_key(update_id)
        acceptance_key = inbox.update_acceptance_key(update_id)
        defn = self._defn.updates.get(envelope["name"]) or self._defn.updates.get(None)
        if defn is None:
            failure = exceptions.ApplicationError(
                f"update handler {envelope['name']!r} not found",
                type="NotFoundError",
                non_retryable=True,
            )
            self._reply(acceptance_key, status="rejected", failure=failure)
            self._reply(reply_key, status="rejected", failure=failure)
            return
        # Decode headers once here (real loop) so the codec runs off the virtual
        # loop; both the validator and the handler task get ready Payloads.
        update_headers = await conversion.decode_headers(envelope.get("headers"))
        if defn.validator is not None:

            def run_validator() -> None:
                # Routed through the inbound chain; the root sets the read-only,
                # against-current-state context (a rejected update must leave no
                # trace in workflow state). current_update_info() resolves here.
                assert self._inbound is not None
                token = _current_update_info.set(
                    UpdateInfo(id=envelope["update_id"], name=envelope["name"])
                )
                try:
                    self._inbound.handle_update_validator(
                        _wfi.HandleUpdateInput(
                            id=self._workflow_id,
                            update=envelope["name"],
                            args=envelope["args"],
                            headers=update_headers,
                        )
                    )
                finally:
                    _current_update_info.reset(token)

            # The verdict is a checkpoint: the validator runs exactly once,
            # at first delivery; replay reads the recorded verdict.
            verdict = await _validate_update(run_validator)
            if not verdict["accepted"]:
                self._reply(
                    acceptance_key,
                    status="rejected",
                    failure_envelope=verdict["failure"],
                )
                self._reply(
                    reply_key, status="rejected", failure_envelope=verdict["failure"]
                )
                return
        # Past validation: the update is accepted (WorkflowUpdateStage
        # ACCEPTED); the handler runs as a tracked vloop task.
        self._reply(acceptance_key, status="accepted")
        self._spawn_handler(
            self._run_update_handler(
                # Incoming update name (not defn.name, None for a dynamic
                # handler): the inbound chain re-resolves it.
                envelope["name"],
                envelope["args"],
                reply_key,
                update_headers,
                envelope["update_id"],
            ),
            kind="update",
            # A dynamic handler has no name of its own; label it by the
            # incoming update name.
            name=defn.name or envelope["name"],
            policy=defn.unfinished_policy,
            handler_id=envelope["update_id"],
        )

    async def _run_update_handler(
        self,
        name: str,
        args: Sequence[Any],
        reply_key: str,
        headers: Mapping[str, Any],
        update_id: str,
    ) -> None:
        # current_update_info() resolves to this update for the handler's life.
        _current_update_info.set(UpdateInfo(id=update_id, name=name))
        try:
            assert self._inbound is not None
            result = await self._inbound.handle_update_handler(
                _wfi.HandleUpdateInput(
                    id=self._workflow_id,
                    update=name,
                    args=args,
                    headers=headers,
                )
            )
        except ContinueAsNewError as can:
            # The update never completes (no result event); like Temporal,
            # prefer initiating CAN from the primary coroutine.
            self._set_outcome(("continue_as_new", can))
            return
        except BaseException as err:  # noqa: BLE001
            if self._is_failure_exception(err):
                # Post-acceptance failure exceptions fail the update, not
                # the workflow (Temporal semantics).
                self._reply(reply_key, status="failed", failure=err)
            else:
                self._record_workflow_error(err)
            return
        self._reply(reply_key, status="completed", result=result)

    async def _apply_query(self, envelope: inbox.Envelope) -> None:
        reply_key = inbox.query_result_key(envelope["request_id"])
        defn = self._defn.queries.get(envelope["name"]) or self._defn.queries.get(None)
        if defn is None:
            failure = exceptions.ApplicationError(
                f"query handler {envelope['name']!r} not found",
                type="NotFoundError",
                non_retryable=True,
            )
            self._reply(reply_key, status="failed", failure=failure)
            return
        # Queries are synchronous (DEVIATIONS #11): the inbound chain is driven
        # to completion without suspension (the root invokes the sync handler).
        self._read_only = True
        try:
            assert self._inbound is not None
            result = await self._inbound.handle_query(
                _wfi.HandleQueryInput(
                    id=self._workflow_id,
                    query=envelope["name"],
                    args=envelope["args"],
                    headers=await conversion.decode_headers(envelope.get("headers")),
                )
            )
        except asyncio.CancelledError:
            # Never swallow cancellation as a query failure: queries are driven
            # synchronously so this is unreachable in practice, but if a query
            # interceptor ever suspended, the cancel must propagate, not become
            # a "failed" reply.
            raise
        except BaseException as err:  # noqa: BLE001
            self._reply(reply_key, status="failed", failure=err)
            return
        finally:
            self._read_only = False
        self._reply(reply_key, status="completed", result=result)

    def _reply(
        self,
        key: str,
        *,
        status: str,
        result: Any = None,
        failure: Optional[BaseException] = None,
        failure_envelope: Optional[Any] = None,
    ) -> None:
        from .payloads import serialize_failure

        payload: Dict[str, Any] = {"status": status}
        if failure_envelope is not None:
            payload["failure"] = failure_envelope
        elif failure is not None:
            payload["failure"] = serialize_failure(failure)
        else:
            payload["result"] = result
        self._outbox.append((key, payload))

    # ------------------------------------------------------------------
    # workflow.py runtime backing (_Runtime)
    # ------------------------------------------------------------------

    def runtime_outbound(self) -> _wfi.WorkflowOutboundInterceptor:
        assert self._outbound is not None, "interceptor chains not built"
        return self._outbound

    def runtime_info(self) -> Info:
        base_id = ids.parse_run(self._workflow_id)[0]
        parent = (
            ParentInfo(
                namespace="default",
                run_id=self._parent_run_id,
                workflow_id=ids.parse_run(self._parent_run_id)[0],
            )
            if self._parent_run_id is not None
            else None
        )
        # Root of this run's tree, threaded in via the child-start meta-envelope
        # (§6.6); None for a top-level workflow (itself the root).
        root = (
            RootInfo(
                run_id=self._meta.root["run_id"],
                workflow_id=self._meta.root["workflow_id"],
            )
            if self._meta.root is not None
            else None
        )
        start_time = datetime.fromtimestamp(self._start_time)
        return Info(
            attempt=self._meta.attempt,
            continued_run_id=self._continued_from,
            cron_schedule=self._meta.cron,
            # Run 0's DBOS id is the bare workflow id (ids.run_dbos_id), i.e.
            # the run-chain base — the first execution of this chain.
            first_execution_run_id=base_id,
            headers=self._headers,
            namespace="default",
            parent=parent,
            root=root,
            retry_policy=(
                deserialize_retry_policy(self._meta.retry_policy)
                if self._meta.retry_policy is not None
                else None
            ),
            run_id=self._workflow_id,
            run_timeout=(
                timedelta(seconds=self._meta.run_timeout)
                if self._meta.run_timeout is not None
                else None
            ),
            search_attributes=_attributes.typed_to_untyped(self._typed_sa),
            start_time=start_time,
            task_queue=self._task_queue_name,
            typed_search_attributes=self._typed_sa,
            workflow_id=self._workflow_id,
            workflow_start_time=start_time,
            workflow_type=self._defn.name,
        )

    def runtime_has_last_completion_result(self) -> bool:
        return self._meta.last_completion is not None

    def runtime_last_completion_result(self, type_hint: Optional[type] = None) -> Any:
        last = self._meta.last_completion
        if last is None:
            return None
        return conversion.decode_value_sync(last["value"], type_hint)

    def runtime_last_failure(self) -> Optional[BaseException]:
        env = self._meta.last_failure
        return deserialize_failure(env) if env is not None else None

    def runtime_memo(self) -> Mapping[str, Any]:
        return dict(self._memo)

    def runtime_memo_value(self, key: str, *, type_hint: Optional[type] = None) -> Any:
        if key not in self._memo:
            raise KeyError(f"Memo does not have a value for key {key}")
        value = self._memo[key]
        if type_hint is None:
            return value
        # The value is already converted (codec-decoded at run start); round-trip
        # it through the sync converter to rebuild it as ``type_hint``.
        return conversion.decode_value_sync(
            conversion.encode_value_sync(value), type_hint
        )

    def runtime_upsert_memo(self, updates: Mapping[str, Any]) -> None:
        self._assert_not_read_only("upsert memo")
        for name, value in updates.items():
            if value is None:
                self._memo.pop(name, None)
            else:
                self._memo[name] = value
        self._commands.append(("attributes", 0))

    def runtime_upsert_search_attributes(
        self,
        attributes: Union[SearchAttributes, Sequence[SearchAttributeUpdate[Any]]],
    ) -> None:
        self._assert_not_read_only("upsert search attributes")
        new_sa = _attributes.apply_sa_updates(self._typed_sa, attributes)
        # Validate eagerly (SA encoding is sync, no codec) so a bad value — e.g.
        # a tz-naive datetime — raises HERE, synchronously at the user's upsert
        # call where their try/except can catch it. Deferring to the
        # "attributes" command flush would surface it as a raw exception that
        # escapes the dispatcher uncatchably and re-raises on every replay.
        # Validating before committing self._typed_sa also leaves state
        # unchanged on failure.
        _attributes.encode_search_attributes(new_sa)
        self._typed_sa = new_sa
        self._commands.append(("attributes", 0))

    def runtime_now(self) -> float:
        return self._vloop.time()

    def runtime_random(self) -> Random:
        return self._random

    def runtime_random_seed(self) -> int:
        return self._seed

    def runtime_register_random_seed_callback(
        self, callback: Callable[[int], None]
    ) -> None:
        # Accepted for parity but intentionally a no-op: our seed is fixed per
        # run, so the callback could never fire (DEVIATIONS D31).
        return None

    def runtime_instance(self) -> Any:
        return self._instance

    async def runtime_start_child_workflow(
        self,
        type_name: str,
        args: Sequence[Any],
        *,
        child_id: Optional[str],
        task_queue: Optional[str],
        parent_close_policy: int,
        cancellation_type: int,
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[TypedSearchAttributes, SearchAttributes]
        ] = None,
        run_timeout: Optional[timedelta] = None,
        retry_policy: Optional[RetryPolicy] = None,
        headers: Optional[Mapping[str, Any]] = None,
    ) -> "ChildWorkflowHandle":
        from .payloads import serialize_retry_policy

        self._assert_not_read_only("start a child workflow")
        seq = self._next_seq("child")
        if child_id is not None:
            # Explicit child ids obey the same reservation as client-side
            # starts (auto ids are exempt: they embed this run's id, which
            # may itself carry a chain suffix — parse_run handles those).
            ids.validate_workflow_id(child_id)
        resolved_id = child_id or f"{self._workflow_id}_{seq}"
        child = _ChildExec(
            seq=seq,
            type_name=type_name,
            child_id=resolved_id,
            args=list(args),
            task_queue=task_queue,
            parent_close_policy=parent_close_policy,
            cancellation_type=cancellation_type,
            start_future=self._vloop.create_future(),
            result_future=self._vloop.create_future(),
            memo=memo,
            search_attributes=search_attributes,
            run_timeout=(
                run_timeout.total_seconds() if run_timeout is not None else None
            ),
            retry_policy=(
                serialize_retry_policy(retry_policy)
                if retry_policy is not None
                else None
            ),
            headers=dict(headers or {}),
        )
        self._pending_children[seq] = child
        self._commands.append(("child", seq))
        child.result_future.add_done_callback(
            lambda fut: (
                self._cancelled_child_seqs.append(seq)
                if fut.cancelled() and seq in self._pending_children
                else None
            )
        )
        # Parks the caller until the start is durable (Temporal: handles
        # resolve on start). The outer loop processes the command.
        await child.start_future
        return ChildWorkflowHandle(self, resolved_id, child.result_future)

    async def runtime_send_to_workflow(
        self, workflow_id: str, envelope: Any, *, resolve_chain: bool = False
    ) -> None:
        self._assert_not_read_only("send to a workflow")
        seq = self._next_seq("send")
        future = self._vloop.create_future()
        self._pending_sends[seq] = (workflow_id, envelope, future, resolve_chain)
        self._commands.append(("send", seq))
        await future

    async def _resolve_own_queue(self) -> None:
        if not self._own_queue_resolved:
            fields = await _safe_status(self._workflow_id)
            self._own_queue_name = fields["queue_name"] if fields else None
            self._own_queue_resolved = True

    async def _resolve_current_run(self, workflow_id: str) -> str:
        """Resolve a Temporal workflow id to its current run's DBOS id
        (§6.4 run chains) via exact-id probes (ids.resolve_latest_run; no
        prefix scan). Each batched lookup is a checkpointed management call,
        and the probe sequence is driven by the recorded results, so the
        resolution replays deterministically.
        """

        async def lookup(dbos_ids: Sequence[str]) -> Dict[str, Any]:
            result: Dict[str, Any] = await _safe_status_list(list(dbos_ids))
            return result

        resolved = await ids.resolve_latest_run(workflow_id, lookup)
        return ids.run_dbos_id(workflow_id, resolved[0]) if resolved else workflow_id

    def runtime_cancellation_reason(self) -> Optional[str]:
        return self._cancel_reason

    def runtime_all_handlers_finished(self) -> bool:
        return not self._inflight_handlers

    def runtime_is_replaying(self) -> bool:
        ctx = get_local_dbos_context()
        if ctx is None:
            return False
        return ctx.function_id < self._replay_horizon

    def runtime_is_read_only(self) -> bool:
        return self._read_only

    def _patch(self, id: str) -> bool:
        """Shared patched()/deprecate_patch() logic (DESIGN §6.8).

        Returns whether the *newer* code path should run, mirroring temporalio:
        true on first (non-replaying) execution or when this patch's marker is
        already in recorded history; false when replaying history that predates
        the patch. The decision is memoized per id and claims no function_id
        itself — so an old in-flight run that never had the call keeps its
        checkpoint sequence and replays the old path. When the newer path is
        taken, a marker write is queued (command order) for durable persistence.

        ``deprecate_patch`` shares this exact path: it too records the marker on
        the newer path (so concurrent old runs keep their checkpoint positions);
        unlike temporalio we don't tag the marker as deprecated, since our scan
        only needs the id.
        """
        self._assert_not_read_only("use patched/deprecate_patch")
        use = self._patches_memoized.get(id)
        if use is not None:
            return use
        use = (not self.runtime_is_replaying()) or (id in self._patches_recorded)
        self._patches_memoized[id] = use
        if use:
            seq = self._next_seq("patch")
            self._pending_patches[seq] = id
            self._commands.append(("patch", seq))
        return use

    def runtime_patched(self, id: str) -> bool:
        return self._patch(id)

    def runtime_deprecate_patch(self, id: str) -> None:
        self._patch(id)

    def runtime_history_length(self) -> int:
        ctx = get_local_dbos_context()
        return ctx.function_id if ctx is not None else 0

    def runtime_can_suggested(self) -> bool:
        return self.runtime_history_length() >= CAN_SUGGESTION_THRESHOLD

    def runtime_get_current_deployment_version(
        self,
    ) -> Optional[WorkerDeploymentVersion]:
        # Deployment name is a process-global set by the Worker; the build_id is
        # read live from the worker's DBOS application_version (post-launch, so
        # it reflects an explicit build_id, the pinned default, or a computed
        # code-hash for auto-versioning) — the version DBOS actually pins
        # recovery/dequeue to, so reported == enforced (DEVIATIONS D29). None
        # when no Worker is active (the in-process dispatcher harness).
        from . import registry

        name = registry.worker_deployment_name
        if name is None:
            return None
        return WorkerDeploymentVersion(name, GlobalParams.app_version)

    def runtime_get_current_details(self) -> str:
        return self._current_details

    def runtime_set_current_details(self, details: str) -> None:
        # A state mutation: disallowed from read-only contexts (queries /
        # update validators), like upsert_memo.
        self._assert_not_read_only("set current details")
        self._current_details = details

    async def runtime_wait_condition(
        self, fn: Callable[[], bool], *, timeout: Optional[float]
    ) -> None:
        fut = self._vloop.create_future()
        self._conditions.append((fn, fut))
        if timeout is not None:
            await asyncio.wait_for(fut, timeout)
        else:
            await fut


def _unfinished_handler_message(kind: str, consequence: str) -> str:
    """Mirrors temporalio's [TMPRL1102] unfinished-handler warning text,
    adapted to our delivery model."""
    return (
        f"[TMPRL1102] Workflow finished while {kind} handlers are still running. "
        f"This may have interrupted work that the {kind} handler was doing, and "
        f"{consequence}. You can wait for all update and signal handlers to "
        "complete by using `await workflow.wait_condition(lambda: "
        "workflow.all_handlers_finished())`. Alternatively, if you are okay with "
        "interrupting running handlers when the workflow finishes, then you can "
        "disable this warning via the handler decorator: "
        f"`@workflow.{kind}(unfinished_policy="
        f"workflow.HandlerUnfinishedPolicy.ABANDON)`. The following {kind}s were "
        "unfinished (and warnings were not disabled for their handler): "
    )

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
from typing import Any, Callable, Deque, Dict, List, Optional, Sequence, Set, Tuple

from dbos import DBOS
from dbos._context import get_local_dbos_context  # see docs/phase0.md

from .. import activity as activity_api
from .. import exceptions
from ..common import RetryPolicy
from ..workflow import (
    ActivityHandle,
    ChildWorkflowHandle,
    ContinueAsNewError,
    HandlerUnfinishedPolicy,
    Info,
    UnfinishedSignalHandlersWarning,
    UnfinishedUpdateHandlersWarning,
    _Runtime,
)
from . import activities as activities_mod
from . import conversion, ids, inbox
from .payloads import (
    FailureEnvelope,
    RunMeta,
    deserialize_failure,
    deserialize_retry_policy,
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
_update_validate_step: Optional[Callable[[Callable[[], None]], Any]] = None


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
    started: bool = False


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
        self._abandoned_tasks: Set["asyncio.Task[Any]"] = set()
        self._pending_children: Dict[int, _ChildExec] = {}
        self._children_registry: List[Dict[str, Any]] = []
        self._cancelled_child_seqs: List[int] = []
        self._pending_sends: Dict[int, Tuple[str, Any, "asyncio.Future[Any]", bool]] = (
            {}
        )
        self._own_queue_name: Optional[str] = None
        self._own_queue_resolved = False
        self._replay_horizon = 0
        self._can_new_run_id: Optional[str] = None
        self._continued_from: Optional[str] = None
        self._random = Random(0)
        self._workflow_id = ""
        self._start_time = 0.0
        # ("ok", result) | ("failure", exc) | ("task_failure", exc)
        self._outcome: Optional[Tuple[str, Any]] = None

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
        if (
            parent is not None
            and ids.parse_run(parent)[0] == ids.parse_run(self._workflow_id)[0]
        ):
            self._continued_from = parent

        init = await _workflow_init_step()
        self._start_time = float(init["start_time"])
        self._vloop.time_seconds = self._start_time
        self._random.seed(init["seed"])

        # Rebuild the typed run arguments from their payloads (deterministic,
        # so re-decoding each run/replay is replay-safe). Tolerant of raw args
        # from the Phase-0 dispatcher helpers (see conversion.decode_values).
        self._args = await conversion.decode_values(self._args, self._defn.arg_types)
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
            if self._outcome is not None and self._outcome[0] != "task_failure":
                # Mark in-flight attempts cancelled BEFORE tearing down
                # their waiter tasks below: task cancellation unregisters
                # the attempt's live context, after which a still-running
                # (threaded) activity function could no longer be reached
                # and would spin forever.
                for exec_state in self._pending_activities.values():
                    activity_api._request_cancel((self._workflow_id, exec_state.seq))
                # Terminal outcome (not a retryable task failure): apply
                # ParentClosePolicy to still-running children.
                await self._sweep_children_on_close()
            if self._outcome is not None and self._outcome[0] == "continue_as_new":
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
            if self._outcome is not None and self._outcome[0] != "task_failure":
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
        from contextlib import nullcontext
        from typing import ContextManager

        from dbos import SetWorkflowID, SetWorkflowTimeout

        from . import registry
        from .payloads import serialize_retry_policy

        type_name = can._tdb_workflow or self._defn.name
        dispatch_fn = registry.dbos_workflow_for(type_name)
        base, index = ids.parse_run(self._workflow_id)
        new_run_id = ids.run_dbos_id(base, index + 1)
        if not self._own_queue_resolved:
            status = await DBOS.get_workflow_status_async(self._workflow_id)
            self._own_queue_name = status.queue_name if status else None
            self._own_queue_resolved = True
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
        if can._tdb_run_timeout is not None:
            carried.run_timeout = can._tdb_run_timeout.total_seconds()
        if can._tdb_retry_policy is not None:
            carried.retry_policy = serialize_retry_policy(can._tdb_retry_policy)
        # The new run's args come from user code, so encode them (the next
        # run's interpreter decodes against its run signature).
        payload = wrap_input(await conversion.encode_values(can._tdb_args), carried)
        # Explicit per-run timeout, else DBOS propagates THIS run's absolute
        # deadline to the next run (see dispatcher._enqueue_next_run).
        timeout_ctx: ContextManager[Any] = (
            SetWorkflowTimeout(carried.run_timeout)
            if carried.run_timeout is not None
            else nullcontext()
        )
        with SetWorkflowID(new_run_id), timeout_ctx:
            if queue is not None:
                await queue.enqueue_async(dispatch_fn, payload)
            else:
                # This run wasn't queue-dispatched (Phase 0 helpers): start
                # the next run directly in-process.
                await DBOS.start_workflow_async(dispatch_fn, payload)
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
            result = await self._defn.run_fn(self._instance, *self._args)
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
            raise RuntimeError(
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
    ) -> ActivityHandle:
        self._assert_not_read_only("start an activity")
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
            # WAIT_CANCELLATION_COMPLETED: confirmed by the in-flight
            # attempt's unwind. (A parked async activity has no attempt to
            # confirm; it degrades to TRY_CANCEL below and the completer
            # learns via the gone-event.)
            exec_state.cancel_requested = True
            activity_api._request_cancel((self._workflow_id, seq))
            return
        exec_state.future.cancel()

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
                    # without a durable sleep (e.g. sleep(0) loops).
                    del self._pending_timers[seq]
                    self._vloop.ready.append(handle)
                    progressed = True
                else:
                    self._launch_waiter("timer", seq, DBOS.sleep_async(real_delay))
            elif kind == "activity":
                self._launch_attempt(self._pending_activities[seq])
            elif kind == "child":
                await self._start_child(self._pending_children[seq])
                progressed = True  # the start future resolved either way
            elif kind == "send":
                target, envelope, future, resolve_chain = self._pending_sends.pop(seq)
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
        return progressed

    async def _start_child(self, child: _ChildExec) -> None:
        """Make the child start durable: enqueue its per-type dispatcher
        under the deterministic child id. DBOS records in-workflow starts
        (and SetWorkflowID re-attaches idempotently), so replay re-attaches
        to the same child instead of spawning a twin.
        """
        from dbos import SetWorkflowID

        from . import registry

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
            if not self._own_queue_resolved:
                status = await DBOS.get_workflow_status_async(self._workflow_id)
                self._own_queue_name = status.queue_name if status else None
                self._own_queue_resolved = True
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
            child_payload = await conversion.encode_values(child.args)
            with SetWorkflowID(child.child_id):
                if child_queue is not None:
                    await child_queue.enqueue_async(dispatch_fn, child_payload)
                else:
                    # Parent wasn't queue-dispatched (Phase 0 helpers):
                    # start the child directly in-process.
                    await DBOS.start_workflow_async(dispatch_fn, child_payload)
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
        self._launch_waiter("child", child.seq, _await_child_result(child.child_id))

    async def _sweep_cancellations(self) -> None:
        """Retire activities and children whose virtual-loop futures were
        cancelled during the drain. Cancellation decisions are deterministic
        workflow state, so this sweep replays identically.

        Activities: TRY_CANCEL (default) cancels the real step task (which
        records nothing — replays like a crash); ABANDON detaches it; WAIT
        is approximated as TRY_CANCEL until Phase 3.

        Children: non-ABANDON types deliver the child's cooperative-cancel
        envelope (a checkpointed send) and retire the waiter; ABANDON just
        retires the waiter. The WAIT variants are approximated (the awaiter
        is already gone once the future is cancelled).
        """
        seqs, self._cancelled_activity_seqs = self._cancelled_activity_seqs, []
        for seq in seqs:
            exec_state = self._pending_activities.pop(seq, None)
            if exec_state is None:
                continue
            if exec_state.cancellation_type != 2:  # ABANDON never requests
                # Mark the (possibly threaded, still-running) attempt so it
                # observes cancellation at its next heartbeat.
                activity_api._request_cancel((self._workflow_id, seq))
            activity_api._forget_attempt_state((self._workflow_id, seq))
            if exec_state.async_pending:
                # Tell the external completer (checkpointed event): its next
                # heartbeat/complete raises instead of vanishing.
                await DBOS.set_event_async(
                    inbox.async_activity_gone_key(exec_state.activity_id), True
                )
            for waiter in list(self._waiters):
                if waiter.kind in ("activity", "act_s2c", "act_hb") and (
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
            "seq": exec_state.seq,
            "workflow_id": ids.parse_run(self._workflow_id)[0],
            "workflow_run_id": self._workflow_id,
            "workflow_type": self._defn.name,
        }
        # The step wrapper assigns its function_id synchronously here.
        coro = step_fn(exec_state.args, exec_state.start_to_close, meta)
        self._launch_waiter("activity", exec_state.seq, coro)

    def _ensure_inbox_waiter(self) -> None:
        if not any(w.kind == "inbox" for w in self._waiters):
            self._launch_waiter(
                "inbox",
                0,
                DBOS.recv_async(inbox.INBOX_TOPIC, inbox.RECV_TIMEOUT_SECONDS),
            )

    async def _flush_outbox(self) -> None:
        # set_event is checkpointed per call: replay re-flushes identically.
        outbox, self._outbox = self._outbox, []
        for key, value in outbox:
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
        status = await DBOS.get_workflow_status_async(current)
        if status is None or status.status not in ("PENDING", "ENQUEUED", "DELAYED"):
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
        """Returns (backoff delay before next attempt, or None to give up;
        retry state to report when giving up).
        """
        policy = exec_state.retry_policy
        if failure.get("non_retryable"):
            return None, exceptions.RetryState.NON_RETRYABLE_FAILURE
        failure_type = failure.get("type") or failure["cls"]
        if policy.non_retryable_error_types and failure_type in set(
            policy.non_retryable_error_types
        ):
            return None, exceptions.RetryState.NON_RETRYABLE_FAILURE
        if policy.maximum_attempts and exec_state.attempt >= policy.maximum_attempts:
            return None, exceptions.RetryState.MAXIMUM_ATTEMPTS_REACHED
        override = failure.get("next_retry_delay")
        if override is not None:
            delay = float(override)
        else:
            delay = policy.initial_interval.total_seconds() * (
                policy.backoff_coefficient ** (exec_state.attempt - 1)
            )
            maximum = (
                policy.maximum_interval.total_seconds()
                if policy.maximum_interval
                else policy.initial_interval.total_seconds() * 100
            )
            delay = min(delay, maximum)
        if exec_state.schedule_to_close is not None:
            elapsed = self._vloop.time() - exec_state.scheduled_at
            if elapsed + delay >= exec_state.schedule_to_close:
                return None, exceptions.RetryState.TIMEOUT
        return delay, exceptions.RetryState.IN_PROGRESS

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
            self._apply_signal(envelope)
        elif kind == "update":
            await self._apply_update(envelope)
        elif kind == "query":
            self._apply_query(envelope)
        elif kind == "activity_result":
            self._apply_activity_result(envelope)
        elif kind == "activity_heartbeat":
            self._apply_activity_heartbeat(envelope)
        elif kind == "cancel":
            self._apply_cancel(envelope)
        else:
            logger.warning(
                "Workflow %s: unknown inbox envelope kind %r", self._workflow_id, kind
            )

    def _async_pending_by_id(self, activity_id: str) -> Optional[_ActivityExec]:
        for exec_state in self._pending_activities.values():
            if exec_state.activity_id == activity_id and exec_state.async_pending:
                return exec_state
        return None

    def _apply_activity_result(self, envelope: inbox.Envelope) -> None:
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
            exec_state.future.set_result(envelope.get("result"))
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

    def _apply_activity_heartbeat(self, envelope: inbox.Envelope) -> None:
        """Heartbeat for an async-pending activity from the external
        completer; refreshes the parked heartbeat-timeout window, and the
        details surface on the next retry attempt (in-process, like
        in-activity heartbeats)."""
        activity_id = str(envelope.get("activity_id", ""))
        exec_state = self._async_pending_by_id(activity_id)
        if exec_state is not None:
            exec_state.async_hb_seen = True
            activity_api._heartbeat_store[(self._workflow_id, exec_state.seq)] = list(
                envelope.get("details", [])
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

    def _apply_signal(self, envelope: inbox.Envelope) -> None:
        defn = self._defn.signals.get(envelope["name"])
        if defn is None:
            # Buffered for delivery if a handler is registered later
            # (dynamic registration arrives in Phase 2).
            self._buffered_signals.setdefault(envelope["name"], []).append(envelope)
            logger.debug(
                "Workflow %s: buffering signal %r with no handler",
                self._workflow_id,
                envelope["name"],
            )
            return
        self._spawn_handler(
            self._run_signal_handler(defn.fn, envelope["args"]),
            kind="signal",
            name=defn.name,
            policy=defn.unfinished_policy,
        )

    async def _run_signal_handler(
        self, fn: Callable[..., Any], args: Sequence[Any]
    ) -> None:
        try:
            result = fn(self._instance, *args)
            if asyncio.iscoroutine(result):
                await result
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
            if record["policy"] == HandlerUnfinishedPolicy.WARN_AND_ABANDON
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
        defn = self._defn.updates.get(envelope["name"])
        if defn is None:
            failure = exceptions.ApplicationError(
                f"update handler {envelope['name']!r} not found",
                type="NotFoundError",
                non_retryable=True,
            )
            self._reply(acceptance_key, status="rejected", failure=failure)
            self._reply(reply_key, status="rejected", failure=failure)
            return
        if defn.validator is not None:
            validator = defn.validator

            def run_validator() -> None:
                # Synchronous, read-only, against current state; a rejected
                # update must leave no trace in workflow state.
                self._read_only = True
                try:
                    validator(self._instance, *envelope["args"])
                finally:
                    self._read_only = False

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
            self._run_update_handler(defn.fn, envelope["args"], reply_key),
            kind="update",
            name=defn.name,
            policy=defn.unfinished_policy,
            handler_id=envelope["update_id"],
        )

    async def _run_update_handler(
        self, fn: Callable[..., Any], args: Sequence[Any], reply_key: str
    ) -> None:
        try:
            result = fn(self._instance, *args)
            if asyncio.iscoroutine(result):
                result = await result
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

    def _apply_query(self, envelope: inbox.Envelope) -> None:
        reply_key = inbox.query_result_key(envelope["request_id"])
        defn = self._defn.queries.get(envelope["name"])
        if defn is None:
            failure = exceptions.ApplicationError(
                f"query handler {envelope['name']!r} not found",
                type="NotFoundError",
                non_retryable=True,
            )
            self._reply(reply_key, status="failed", failure=failure)
            return
        self._read_only = True
        try:
            result = defn.fn(self._instance, *envelope["args"])
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

    def runtime_info(self) -> Info:
        return Info(
            attempt=self._meta.attempt,
            continued_run_id=self._continued_from,
            cron_schedule=self._meta.cron,
            namespace="default",
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
            start_time=datetime.fromtimestamp(self._start_time),
            task_queue="default",
            workflow_id=self._workflow_id,
            workflow_type=self._defn.name,
        )

    def runtime_has_last_completion_result(self) -> bool:
        return self._meta.last_completion is not None

    def runtime_last_completion_result(self) -> Any:
        last = self._meta.last_completion
        return last["value"] if last is not None else None

    def runtime_last_failure(self) -> Optional[BaseException]:
        env = self._meta.last_failure
        return deserialize_failure(env) if env is not None else None

    def runtime_now(self) -> float:
        return self._vloop.time()

    def runtime_random(self) -> Random:
        return self._random

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

    async def _resolve_current_run(self, workflow_id: str) -> str:
        """Resolve a Temporal workflow id to its current run's DBOS id
        (§6.4 run chains) via exact-id probes (ids.resolve_latest_run; no
        prefix scan). Each batched lookup is a checkpointed management call,
        and the probe sequence is driven by the recorded results, so the
        resolution replays deterministically.
        """

        async def lookup(dbos_ids: Sequence[str]) -> Dict[str, Any]:
            statuses = await DBOS.list_workflows_async(workflow_ids=list(dbos_ids))
            return {status.workflow_id: status for status in statuses}

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

    def runtime_history_length(self) -> int:
        ctx = get_local_dbos_context()
        return ctx.function_id if ctx is not None else 0

    def runtime_can_suggested(self) -> bool:
        return self.runtime_history_length() >= CAN_SUGGESTION_THRESHOLD

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

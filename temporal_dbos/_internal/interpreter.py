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
import logging
import secrets
import time as time_mod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from random import Random
from typing import Any, Callable, Deque, Dict, List, Optional, Sequence, Set, Tuple

from dbos import DBOS
from dbos._context import get_local_dbos_context  # see docs/phase0.md

from .. import exceptions
from ..common import RetryPolicy
from ..workflow import ActivityHandle, ChildWorkflowHandle, Info, _Runtime
from . import activities as activities_mod
from . import ids, inbox
from .payloads import FailureEnvelope, deserialize_failure
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

# Created lazily (not at import) so decoration binds to the live DBOS
# registry — tests destroy and re-create it between cases.
_init_step: Optional[Callable[[], Any]] = None
_child_result_step: Optional[Callable[[str], Any]] = None


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
                SerializedWorkflowCancellation,
                SerializedWorkflowFailure,
                serialize_failure,
            )

            dbos = _get_dbos_instance()
            try:
                result = await dbos._sys_db.await_workflow_result_async(
                    child_id, CHILD_POLL_INTERVAL_SECONDS
                )
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

    def __init__(self, defn: WorkflowDefinition, args: Sequence[Any]) -> None:
        self._defn = defn
        self._args = list(args)
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
        self._handlers_running = 0
        self._read_only = False
        self._cancel_requested = False
        self._cancel_reason: Optional[str] = None
        self._cancelled_activity_seqs: List[int] = []
        self._abandoned_tasks: Set["asyncio.Task[Any]"] = set()
        self._pending_children: Dict[int, _ChildExec] = {}
        self._children_registry: List[Dict[str, Any]] = []
        self._cancelled_child_seqs: List[int] = []
        self._pending_sends: Dict[int, Tuple[str, Any, "asyncio.Future[Any]"]] = {}
        self._own_queue_name: Optional[str] = None
        self._own_queue_resolved = False
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

        init = await _workflow_init_step()
        self._start_time = float(init["start_time"])
        self._vloop.time_seconds = self._start_time
        self._random.seed(init["seed"])

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
                self._deliver(done)
        finally:
            if self._outcome is not None and self._outcome[0] != "task_failure":
                # Terminal outcome (not a retryable task failure): apply
                # ParentClosePolicy to still-running children.
                await self._sweep_children_on_close()
            for waiter in self._waiters:
                waiter.task.cancel()
            if self._waiters:
                await asyncio.gather(
                    *(w.task for w in self._waiters), return_exceptions=True
                )
            self._waiters.clear()

        if self._handlers_running:
            logger.warning(
                "Workflow %s finished with %d signal/update handler(s) still "
                "running; their effects after this point are lost",
                self._workflow_id,
                self._handlers_running,
            )
        kind, value = self._outcome
        if kind == "ok":
            return value
        if kind == "cancelled":
            raise WorkflowCancelled(value)
        assert kind == "failure"
        raise value  # a FailureError; recorded by DBOS as the workflow error

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
    ) -> ActivityHandle:
        self._assert_not_read_only("start an activity")
        activities_mod.attempt_step_for(activity_name)  # raise early if unknown
        seq = self._next_seq("activity")
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
            activity_id=activity_id or f"{seq}",
            scheduled_at=self._vloop.time(),
            future=self._vloop.create_future(),
            cancellation_type=cancellation_type,
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
        return ActivityHandle(exec_state.future)

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
                target, envelope, future = self._pending_sends.pop(seq)
                # send_async is checkpointed; awaited inline so its
                # function_id claim stays at a deterministic position.
                try:
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
            with SetWorkflowID(child.child_id):
                if child_queue is not None:
                    await child_queue.enqueue_async(dispatch_fn, list(child.args))
                else:
                    # Parent wasn't queue-dispatched (Phase 0 helpers):
                    # start the child directly in-process.
                    await DBOS.start_workflow_async(dispatch_fn, list(child.args))
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
            for waiter in list(self._waiters):
                if waiter.kind == "activity" and waiter.seq == seq:
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
                await DBOS.send_async(
                    child.child_id, inbox.cancel_envelope(), inbox.INBOX_TOPIC
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
            "workflow_id": self._workflow_id,
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

    def _deliver(self, done: Set["asyncio.Task[Any]"]) -> None:
        for waiter in list(self._waiters):
            if waiter.task not in done:
                continue
            self._waiters.remove(waiter)
            if waiter.kind == "inbox":
                self._deliver_inbox(waiter.task.result())
            elif waiter.kind == "timer":
                self._deliver_timer(waiter.seq)
            elif waiter.kind == "activity":
                self._deliver_activity_event(waiter)
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
        if envelope["ok"]:
            del self._pending_activities[waiter.seq]
            exec_state.future.set_result(envelope["result"])
            return
        failure: FailureEnvelope = envelope["failure"]
        exec_state.last_failure = failure
        retry_delay, retry_state = self._retry_decision(exec_state, failure)
        if retry_delay is not None:
            exec_state.in_backoff = True
            self._launch_waiter(
                "activity", exec_state.seq, DBOS.sleep_async(retry_delay)
            )
            return
        del self._pending_activities[waiter.seq]
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
                await DBOS.send_async(
                    child.child_id, inbox.cancel_envelope(), inbox.INBOX_TOPIC
                )
            else:  # TERMINATE (1) and UNSPECIFIED (0) default to terminate
                await self._terminate_child_tree(child.child_id, set())

    async def _terminate_child_tree(self, child_id: str, visited: Set[str]) -> None:
        """Terminate a child and apply its recorded parent-close policies to
        its own descendants (it runs no code, so nobody else will)."""
        if child_id in visited:
            return
        visited.add(child_id)
        status = await DBOS.get_workflow_status_async(child_id)
        if status is None or status.status not in ("PENDING", "ENQUEUED", "DELAYED"):
            return  # already terminal (or stuck); don't clobber its status
        await DBOS.cancel_workflow_async(child_id)
        grandchildren = await DBOS.get_event_async(
            child_id, inbox.CHILDREN_EVENT_KEY, 0
        )
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

    def _deliver_inbox(self, message: Any) -> None:
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
            self._apply_update(envelope)
        elif kind == "query":
            self._apply_query(envelope)
        elif kind == "cancel":
            self._apply_cancel(envelope)
        else:
            logger.warning(
                "Workflow %s: unknown inbox envelope kind %r", self._workflow_id, kind
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
        self._spawn_handler(self._run_signal_handler(defn.fn, envelope["args"]))

    async def _run_signal_handler(
        self, fn: Callable[..., Any], args: Sequence[Any]
    ) -> None:
        try:
            result = fn(self._instance, *args)
            if asyncio.iscoroutine(result):
                await result
        except BaseException as err:  # noqa: BLE001
            # Same classification as the primary coroutine (Temporal
            # semantics: a failure exception in a signal handler fails the
            # workflow; anything else fails the workflow task).
            self._record_workflow_error(err)

    def _spawn_handler(self, coro: Any) -> None:
        self._handlers_running += 1
        task: asyncio.Task[Any] = asyncio.Task(coro, loop=self._vloop)
        self._tasks.add(task)

        def _finished(t: "asyncio.Task[Any]") -> None:
            self._handlers_running -= 1
            self._tasks.discard(t)

        task.add_done_callback(_finished)

    def _apply_update(self, envelope: inbox.Envelope) -> None:
        update_id: str = envelope["update_id"]
        if update_id in self._seen_update_ids:
            return  # duplicate delivery; the original reply event stands
        self._seen_update_ids.add(update_id)
        reply_key = inbox.update_result_key(update_id)
        defn = self._defn.updates.get(envelope["name"])
        if defn is None:
            failure = exceptions.ApplicationError(
                f"update handler {envelope['name']!r} not found",
                type="NotFoundError",
                non_retryable=True,
            )
            self._reply(reply_key, status="rejected", failure=failure)
            return
        if defn.validator is not None:
            # Validators run synchronously, read-only, against current state;
            # a rejected update must leave no trace in workflow state.
            self._read_only = True
            try:
                defn.validator(self._instance, *envelope["args"])
            except BaseException as err:  # noqa: BLE001
                self._reply(reply_key, status="rejected", failure=err)
                return
            finally:
                self._read_only = False
        self._spawn_handler(
            self._run_update_handler(defn.fn, envelope["args"], reply_key)
        )

    async def _run_update_handler(
        self, fn: Callable[..., Any], args: Sequence[Any], reply_key: str
    ) -> None:
        try:
            result = fn(self._instance, *args)
            if asyncio.iscoroutine(result):
                result = await result
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
    ) -> None:
        from .payloads import serialize_failure

        payload: Dict[str, Any] = {"status": status}
        if failure is not None:
            payload["failure"] = serialize_failure(failure)
        else:
            payload["result"] = result
        self._outbox.append((key, payload))

    # ------------------------------------------------------------------
    # workflow.py runtime backing (_Runtime)
    # ------------------------------------------------------------------

    def runtime_info(self) -> Info:
        return Info(
            attempt=1,
            namespace="default",
            run_id=self._workflow_id,
            start_time=datetime.fromtimestamp(self._start_time),
            task_queue="default",
            workflow_id=self._workflow_id,
            workflow_type=self._defn.name,
        )

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
        resolved_id = child_id or f"{self._workflow_id}_{seq}"
        ids.validate_workflow_id(resolved_id)
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

    async def runtime_send_to_workflow(self, workflow_id: str, envelope: Any) -> None:
        self._assert_not_read_only("send to a workflow")
        seq = self._next_seq("send")
        future = self._vloop.create_future()
        self._pending_sends[seq] = (workflow_id, envelope, future)
        self._commands.append(("send", seq))
        await future

    def runtime_cancellation_reason(self) -> Optional[str]:
        return self._cancel_reason

    def runtime_is_replaying(self) -> bool:
        # TODO(phase 2): derive from checkpoint-cursor position to back
        # workflow.unsafe.is_replaying() and replay log suppression.
        return False

    async def runtime_wait_condition(
        self, fn: Callable[[], bool], *, timeout: Optional[float]
    ) -> None:
        fut = self._vloop.create_future()
        self._conditions.append((fn, fut))
        if timeout is not None:
            await asyncio.wait_for(fut, timeout)
        else:
            await fut

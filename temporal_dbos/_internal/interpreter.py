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
from ..workflow import ActivityHandle, Info, _Runtime
from . import activities as activities_mod
from . import inbox
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


class _AbortDrain(Exception):
    """Internal: an exception escaped a non-task callback (e.g. a
    wait_condition predicate) during a virtual-loop drain.
    """

    def __init__(self, cause: BaseException) -> None:
        super().__init__(repr(cause))
        self.cause = cause


# Created lazily (not at import) so decoration binds to the live DBOS
# registry — tests destroy and re-create it between cases.
_init_step: Optional[Callable[[], Any]] = None


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
        logger.error("Workflow virtual loop exception: %s", context.get("message"))

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
    attempt: int = 1
    in_backoff: bool = False
    last_failure: Optional[FailureEnvelope] = None


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
                self._drain()
                made_progress = self._process_commands()
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
        except BaseException as err:  # noqa: BLE001 — classified below
            self._record_workflow_error(err)

    def _record_workflow_error(self, err: BaseException) -> None:
        if self._is_failure_exception(err):
            self._set_outcome(("failure", err))
        else:
            self._set_outcome(("task_failure", err))

    def _set_outcome(self, outcome: Tuple[str, Any]) -> None:
        # First outcome wins; a later handler crash can't overwrite a result.
        if self._outcome is None:
            self._outcome = outcome

    def _is_failure_exception(self, err: BaseException) -> bool:
        return (
            isinstance(err, exceptions.FailureError)
            or isinstance(err, asyncio.TimeoutError)
            or isinstance(err, self._defn.failure_exception_types)
        )

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
        )
        self._pending_activities[seq] = exec_state
        self._commands.append(("activity", seq))
        return ActivityHandle(exec_state.future)

    def _process_commands(self) -> bool:
        """Turn queued commands into real-loop waiter tasks. Returns True if
        any virtual-loop progress was made without needing a checkpointed
        wait (expired timers firing immediately).
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
        return progressed

    def _launch_waiter(self, kind: str, seq: int, coro: Any) -> None:
        task = asyncio.ensure_future(coro)
        self._waiters.append(_Waiter(kind=kind, seq=seq, task=task))

    def _launch_attempt(self, exec_state: _ActivityExec) -> None:
        step_fn = activities_mod.attempt_step_for(exec_state.activity_name)
        exec_state.in_backoff = False
        # The step wrapper assigns its function_id synchronously here.
        coro = step_fn(exec_state.args, exec_state.start_to_close)
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

    def _deliver_timer(self, seq: int) -> None:
        handle = self._pending_timers.pop(seq, None)
        if handle is None:
            return  # cancelled while its waiter was completing
        self._advance_time(handle.when())
        self._vloop.ready.append(handle)

    def _deliver_activity_event(self, waiter: _Waiter) -> None:
        exec_state = self._pending_activities[waiter.seq]
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
            logger.warning(
                "Workflow %s: cancellation requested but cooperative cancel "
                "is not implemented until Phase 2; ignoring",
                self._workflow_id,
            )
        else:
            logger.warning(
                "Workflow %s: unknown inbox envelope kind %r", self._workflow_id, kind
            )

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

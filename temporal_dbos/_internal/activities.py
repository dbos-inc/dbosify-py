"""Activity execution: one single-attempt DBOS step per activity type.

Each registered activity gets its own step function named ``act:{type}``
(decision §10.1: per-type naming keeps DBOS-native step listings readable
and makes replay-mismatch detection precise). The step body resolves the
activity from the registry at execution time, so re-registering an activity
implementation takes effect without re-decoration.

The step *always returns an envelope* — user exceptions are caught inside
the step body and serialized through the failure serializer — so checkpoint
contents are fully under our control and the interpreter's retry machinery
(not DBOS's) decides what happens next. ``ended_at`` rides in the envelope
to advance the workflow's virtual clock deterministically.
"""

import asyncio
import time as time_mod
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

from dbos import DBOS

from .. import exceptions
from ..common import RetryPolicy
from . import registry
from .payloads import FailureEnvelope, serialize_failure

AttemptStep = Callable[
    [List[Any], Optional[float], Dict[str, Any]], Coroutine[Any, Any, Dict[str, Any]]
]


def activity_api_complete_async_error() -> "type[BaseException]":
    from .. import activity as activity_api

    return activity_api._CompleteAsyncError


_attempt_steps: Dict[str, AttemptStep] = {}


def ensure_attempt_step(activity_name: str) -> None:
    if activity_name in _attempt_steps:
        return
    _attempt_steps[activity_name] = _make_attempt_step(activity_name)


def attempt_step_for(activity_name: str) -> AttemptStep:
    step = _attempt_steps.get(activity_name)
    if step is None:
        raise KeyError(
            f"Activity type {activity_name!r} is not registered with this worker. "
            f"Registered types: {sorted(_attempt_steps)}"
        )
    return step


def retry_decision(
    policy: RetryPolicy,
    attempt: int,
    failure: FailureEnvelope,
    *,
    elapsed: Optional[float],
    schedule_to_close: Optional[float],
) -> Tuple[Optional[float], exceptions.RetryState]:
    """The activity retry decision (DESIGN §6.1.2), clock-independent so both
    execution paths share it: the local path (interpreter, virtual time) and the
    queued path (the ``__temporal_activity`` workflow, recorded-timestamp time).

    Returns ``(backoff delay before the next attempt, or None to give up;
    the retry state to report when giving up)``. ``elapsed`` is the time since
    the activity was scheduled; pass it (with ``schedule_to_close``) so the
    schedule-to-close budget gates retries — like Temporal, it bounds the retry
    sequence, not an in-flight attempt (``start_to_close`` bounds that).
    """
    if failure.get("non_retryable"):
        return None, exceptions.RetryState.NON_RETRYABLE_FAILURE
    failure_type = failure.get("type") or failure["cls"]
    if policy.non_retryable_error_types and failure_type in set(
        policy.non_retryable_error_types
    ):
        return None, exceptions.RetryState.NON_RETRYABLE_FAILURE
    if policy.maximum_attempts and attempt >= policy.maximum_attempts:
        return None, exceptions.RetryState.MAXIMUM_ATTEMPTS_REACHED
    override = failure.get("next_retry_delay")
    if override is not None:
        delay = float(override)
    else:
        delay = policy.initial_interval.total_seconds() * (
            policy.backoff_coefficient ** (attempt - 1)
        )
        maximum = (
            policy.maximum_interval.total_seconds()
            if policy.maximum_interval
            else policy.initial_interval.total_seconds() * 100
        )
        delay = min(delay, maximum)
    if schedule_to_close is not None and elapsed is not None:
        if elapsed + delay >= schedule_to_close:
            return None, exceptions.RetryState.TIMEOUT
    return delay, exceptions.RetryState.IN_PROGRESS


def _make_attempt_step(activity_name: str) -> AttemptStep:
    async def attempt(
        args: List[Any], start_to_close: Optional[float], meta: Dict[str, Any]
    ) -> Dict[str, Any]:
        from .. import activity as activity_api

        defn = registry.lookup_activity(activity_name)

        attempt_started_at = time_mod.time()
        attempt_key = (str(meta.get("workflow_run_id", "")), int(meta.get("seq", -1)))
        heartbeat_timeout = meta.get("heartbeat_timeout")
        # The activity context (activity.info()/heartbeat()) rides a
        # contextvar; asyncio.to_thread copies the context, so sync
        # activities see it too. Registering the context lets the
        # interpreter deliver cancellation into a running attempt (the
        # threading.Event outlives task cancellation, so an abandoned
        # sync thread still observes it at its next heartbeat).
        ctx = activity_api._Context(
            info=activity_api._make_info(meta),
            on_heartbeat=lambda *details: None,
            attempt_key=attempt_key,
        )

        async def call_user_activity() -> Dict[str, Any]:
            from . import conversion

            decoded_args = await conversion.decode_values(args, defn.arg_types)
            activity_api._register_attempt(attempt_key, ctx)
            token = activity_api._current_context.set(ctx)
            try:
                if defn.is_async:
                    result = await defn.fn(*decoded_args)
                else:
                    result = await asyncio.to_thread(defn.fn, *decoded_args)
            except Exception as err:  # noqa: BLE001 — serialized, not swallowed
                return {
                    "ok": False,
                    "failure": serialize_failure(err),
                    "ended_at": time_mod.time(),
                }
            finally:
                activity_api._current_context.reset(token)
                activity_api._unregister_attempt(attempt_key, ctx)
            return {
                "ok": True,
                "result": await conversion.encode_value(result),
                "ended_at": time_mod.time(),
            }

        async def run_attempt() -> Dict[str, Any]:
            if heartbeat_timeout is None:
                return await call_user_activity()
            # Heartbeat-timeout watchdog (Temporal's liveness contract): an
            # attempt that stops heartbeating for longer than the timeout
            # fails with TimeoutType.HEARTBEAT (and retries per policy). The
            # hung function is marked cancelled — a still-live thread
            # unwinds at its next heartbeat — and abandoned, like
            # start-to-close enforcement.
            task = asyncio.ensure_future(call_user_activity())
            poll = max(0.05, float(heartbeat_timeout) / 4)
            try:
                return await _watch(task, poll)
            except asyncio.CancelledError:
                # asyncio.wait does NOT cancel what it waits on: propagate
                # explicitly so TRY_CANCEL / start-to-close actually stop an
                # async activity function rather than orphaning it.
                task.cancel()
                raise

        async def _watch(
            task: "asyncio.Task[Dict[str, Any]]", poll: float
        ) -> Dict[str, Any]:
            assert heartbeat_timeout is not None
            while True:
                done, _ = await asyncio.wait({task}, timeout=poll)
                if done:
                    return task.result()
                stale = time_mod.monotonic() - ctx.last_heartbeat_at
                if stale > float(heartbeat_timeout):
                    ctx.cancelled.set()
                    task.cancel()
                    hb_timeout = exceptions.TimeoutError(
                        "activity Heartbeat timeout",
                        type=exceptions.TimeoutType.HEARTBEAT,
                        last_heartbeat_details=list(ctx.last_heartbeat),
                    )
                    return {
                        "ok": False,
                        "failure": serialize_failure(hb_timeout),
                        "ended_at": time_mod.time(),
                    }

        try:
            # User exceptions (including user-raised TimeoutError) are
            # converted inside call_user_activity, so a TimeoutError here is
            # unambiguously the start-to-close enforcement firing.
            return await asyncio.wait_for(run_attempt(), timeout=start_to_close)
        except activity_api_complete_async_error():
            # raise_complete_async(): the function returned, but the
            # activity stays pending until externally completed (the
            # checkpointed marker makes the parked state replay-stable).
            # started_at lets the interpreter arm the *remaining*
            # start-to-close for the parked wait (per-attempt, as in
            # Temporal).
            return {
                "async_pending": True,
                "started_at": attempt_started_at,
                "ended_at": time_mod.time(),
            }
        except (asyncio.TimeoutError, TimeoutError):
            # Mark the context so a hung sync thread (which cancellation
            # cannot interrupt) still unwinds at its next heartbeat.
            ctx.cancelled.set()
            timeout_failure = exceptions.TimeoutError(
                "activity Start-To-Close timeout",
                type=exceptions.TimeoutType.START_TO_CLOSE,
                last_heartbeat_details=[],
            )
            return {
                "ok": False,
                "failure": serialize_failure(timeout_failure),
                "ended_at": time_mod.time(),
            }

    attempt.__name__ = attempt.__qualname__ = f"act:{activity_name}"
    decorated: AttemptStep = DBOS.step(name=f"act:{activity_name}")(attempt)
    return decorated

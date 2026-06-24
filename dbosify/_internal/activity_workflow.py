"""The ``__temporal_activity`` dispatcher: the cross-queue / distributed
activity path.

When ``execute_activity(..., task_queue=)`` names a queue other than the
workflow's own, the interpreter does not run the activity as an in-process
``act:{type}`` step (the local path in ``activities.py``). Instead it enqueues
*this* DBOS workflow onto that DBOS queue, so the activity runs on whatever
worker listens there — Temporal's "activities run on different workers".

The workflow id is the deterministic ``{parent_dbos_id}-a{seq}`` chosen by the
interpreter; the interpreter awaits this workflow's result envelope exactly as
it awaits a child workflow's (``interpreter._await_activity_result``).

By design (Design A) the activity workflow owns the full activity lifecycle on
its own worker: the retry loop with durable backoff, schedule-to-close /
schedule-to-start enforcement, the heartbeat-timeout watchdog and cross-process
cancellation poll (both inside the attempt step), and the raise_complete_async
park for external completion.
"""

import logging
import time as time_mod
from typing import Any, Callable, Dict, Optional

from dbos import DBOS

from .. import exceptions
from ..common import RetryPolicy
from . import activities as activities_mod
from . import inbox, registry
from .payloads import deserialize_retry_policy, serialize_failure

logger = logging.getLogger("dbosify.activity_workflow")

ACTIVITY_DISPATCH_NAME = "__temporal_activity"

_activity_dispatcher_registered = False
_started_at_step: Optional[Callable[[], Any]] = None
_created_at_step: Optional[Callable[[str], Any]] = None


def _activity_started_at() -> Any:
    """A step that records the activity workflow's start wall-clock, once, so
    the retry loop measures schedule-to-close elapsed deterministically (the
    recorded value replays identically — no live clock read in the loop)."""
    global _started_at_step
    if _started_at_step is None:

        @DBOS.step(name="__dbosify_activity_started_at")
        async def started_at() -> float:
            return time_mod.time()

        _started_at_step = started_at
    return _started_at_step()


def _activity_created_at(workflow_id: str) -> Any:
    """A step reading this activity workflow's enqueue time (``created_at``, ms)
    for the schedule-to-start check. Checkpointed, so it replays identically."""
    global _created_at_step
    if _created_at_step is None:

        @DBOS.step(name="__dbosify_activity_created_at")
        async def created_at(workflow_id: str) -> Optional[float]:
            status = await DBOS.get_workflow_status_async(workflow_id)
            if status is None or status.created_at is None:
                return None
            return float(status.created_at) / 1000.0  # ms -> s

        _created_at_step = created_at
    return _created_at_step(workflow_id)


async def _run_queued_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Run a queued activity to a terminal outcome and return its envelope.

    Design A: the activity workflow owns the full retry loop on its own worker,
    so attempt counting, backoff (durable ``DBOS.sleep_async`` — a crash
    mid-backoff resumes at the right attempt), and the schedule-to-close budget
    all live here, reusing ``activities.retry_decision`` (shared with the local
    path). The single-attempt step (``activities._make_attempt_step``) returns a
    fully-formed envelope with user exceptions serialized inside it, so this
    workflow never raises on a user failure; it returns the terminal envelope as
    its DBOS output (with ``retry_state`` stamped in when retries are exhausted).
    """
    activity_name = payload["activity_name"]
    args = payload["args"]
    start_to_close = payload.get("start_to_close")
    schedule_to_close = payload.get("schedule_to_close")
    serialized_policy = payload.get("retry_policy")
    policy = (
        deserialize_retry_policy(serialized_policy)
        if serialized_policy is not None
        else RetryPolicy()
    )
    meta = dict(payload["meta"])
    # Surface the activity's timeouts/retry policy to activity.info(); on this
    # path they live at payload level, so copy them into meta.
    meta["start_to_close"] = start_to_close
    meta["schedule_to_close"] = schedule_to_close
    meta["retry_policy"] = serialized_policy
    # ``attempt_step_for`` raises a clear KeyError if the activity type is not
    # registered with *this* worker — the correct failure for a misrouted task_queue.
    step_fn = activities_mod.attempt_step_for(activity_name)
    started_at = await _activity_started_at()

    # schedule_to_start: if the activity sat in the queue past the budget, fail
    # before running any attempt (terminal — the activity never started).
    schedule_to_start = payload.get("schedule_to_start")
    if schedule_to_start is not None:
        from dbos._context import get_local_dbos_context

        ctx = get_local_dbos_context()
        if ctx is not None:
            created_at = await _activity_created_at(ctx.workflow_id)
            if created_at is not None and (started_at - created_at) > schedule_to_start:
                timed_out = exceptions.TimeoutError(
                    "activity Schedule-To-Start timeout",
                    type=exceptions.TimeoutType.SCHEDULE_TO_START,
                    last_heartbeat_details=[],
                )
                return {
                    "ok": False,
                    "failure": serialize_failure(timed_out),
                    "ended_at": started_at,
                    "retry_state": int(exceptions.RetryState.TIMEOUT),
                }

    attempt = int(meta.get("attempt", 1))
    # Budget consumed before this attempt, reconstructed deterministically from recorded ended_at + durable backoffs (measured from started_at).
    elapsed_before = 0.0
    while True:
        meta["attempt"] = attempt
        # Bound this attempt by min(start_to_close, remaining schedule_to_close).
        remaining_stc = (
            schedule_to_close - elapsed_before
            if schedule_to_close is not None
            else None
        )
        deadline, deadline_type = activities_mod.effective_deadline(
            start_to_close, remaining_stc
        )
        meta["deadline_type"] = deadline_type
        envelope: Dict[str, Any] = await step_fn(args, deadline, meta)
        if envelope.get("async_pending"):
            # Park for completion, minus the body time already spent against the deadline.
            if deadline is not None:
                body_elapsed = float(envelope.get("ended_at", 0.0)) - float(
                    envelope.get("started_at", envelope.get("ended_at", 0.0))
                )
                wait_timeout = max(0.0, deadline - max(0.0, body_elapsed))
            else:
                wait_timeout = inbox.RECV_TIMEOUT_SECONDS
            envelope = await _await_async_completion(
                envelope, wait_timeout, str(meta.get("activity_id", "")), deadline_type
            )
        if envelope.get("ok"):
            return envelope
        failure = envelope["failure"]
        if failure.get("cls") == "CancelledError":
            # Cancellation is terminal — a cancelled activity is never retried
            # (Temporal semantics). The interpreter resolves the awaiter as cancelled.
            return envelope
        elapsed = float(envelope.get("ended_at", started_at)) - started_at
        delay, retry_state = activities_mod.retry_decision(
            policy,
            attempt,
            failure,
            elapsed=elapsed,
            schedule_to_close=schedule_to_close,
        )
        if delay is None:
            return {**envelope, "retry_state": int(retry_state)}
        await DBOS.sleep_async(delay)
        elapsed_before = elapsed + delay
        attempt += 1


async def _await_async_completion(
    pending_env: Dict[str, Any],
    timeout: float,
    activity_id: str,
    deadline_type: str = "start_to_close",
) -> Dict[str, Any]:
    """Park the activity workflow for external completion (raise_complete_async
    on the queued path). Returns a normal attempt envelope: complete -> ok,
    fail -> a retryable failure (the caller re-runs per policy), report_cancellation
    (or a cancellation marker from the interpreter) -> a terminal CancelledError,
    and a recv timeout -> a START_TO_CLOSE / SCHEDULE_TO_CLOSE timeout (per
    ``deadline_type``).

    Heartbeats sent by the completer are not completions: they are skipped so the
    park keeps waiting (detail forwarding to the workflow side remains a
    documented gap). On a definitely-terminal outcome (complete / cancel) we
    set the gone-event so a later completer raises rather than sending into the
    void.

    Timestamps come from recorded values (the completer's ``sent_at`` or the
    parked attempt's ``ended_at``), never a live clock read in the workflow body.
    """
    ended_at = float(pending_env.get("ended_at", 0.0))
    while True:
        completion = await DBOS.recv_async(inbox.ASYNC_COMPLETE_TOPIC, timeout)
        if completion is None:
            is_stc = deadline_type == "schedule_to_close"
            timed_out = exceptions.TimeoutError(
                (
                    "activity Schedule-To-Close timeout"
                    if is_stc
                    else "activity Start-To-Close timeout"
                ),
                type=(
                    exceptions.TimeoutType.SCHEDULE_TO_CLOSE
                    if is_stc
                    else exceptions.TimeoutType.START_TO_CLOSE
                ),
                last_heartbeat_details=[],
            )
            return {
                "ok": False,
                "failure": serialize_failure(timed_out),
                "ended_at": ended_at,
            }
        if completion.get("kind") == "activity_heartbeat":
            # A heartbeat keeps the parked activity alive but is not a completion.
            continue
        ended_at = float(completion.get("sent_at", ended_at))
        if completion.get("cancelled"):
            await DBOS.set_event_async(inbox.async_activity_gone_key(activity_id), True)
            cancelled = exceptions.CancelledError("Activity cancelled")
            return {
                "ok": False,
                "failure": serialize_failure(cancelled),
                "ended_at": ended_at,
            }
        if completion.get("ok"):
            await DBOS.set_event_async(inbox.async_activity_gone_key(activity_id), True)
            return {
                "ok": True,
                "result": completion.get("result"),
                "ended_at": ended_at,
            }
        # An external fail: hand it back so the caller's loop retries per policy
        # (which may re-run the activity and park again).
        return {
            "ok": False,
            "failure": completion["failure"],
            "ended_at": ended_at,
        }


def register_activity_dispatcher() -> None:
    """Register the ``__temporal_activity`` dispatcher (idempotent per process).

    Called from ``dispatcher.register_worker`` for *every* worker — including
    activities-only workers (no ``workflows=``) — so any worker that hosts an
    activity can also dequeue and run queued activities targeted at its queue.
    """
    global _activity_dispatcher_registered
    if _activity_dispatcher_registered:
        return

    async def run_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
        return await _run_queued_activity(payload)

    run_activity.__name__ = run_activity.__qualname__ = ACTIVITY_DISPATCH_NAME
    decorated = DBOS.workflow(name=ACTIVITY_DISPATCH_NAME)(run_activity)
    registry.register_activity_dispatcher_fn(decorated)
    _activity_dispatcher_registered = True

"""The ``__temporal_activity`` dispatcher: the cross-queue / distributed
activity path (DESIGN.md §6.1.2).

When ``execute_activity(..., task_queue=)`` names a queue other than the
workflow's own, the interpreter does not run the activity as an in-process
``act:{type}`` step (the local path in ``activities.py``). Instead it enqueues
*this* DBOS workflow onto that DBOS queue, so the activity runs on whatever
worker listens there — Temporal's "activities run on different workers".

The workflow id is the deterministic ``{parent_dbos_id}-a{seq}`` chosen by the
interpreter; the interpreter awaits this workflow's result envelope exactly as
it awaits a child workflow's (``interpreter._await_activity_result``).

Phase 1 runs a **single attempt** and returns its envelope. The retry loop,
per-attempt/schedule timeouts, heartbeat ownership, and async-completion park
(the rest of §6.1.2 for the queued path) are layered on in later phases — they
live here because, by design (Design A), the activity workflow owns the full
activity lifecycle on its own worker.
"""

import logging
import time as time_mod
from typing import Any, Callable, Dict, Optional

from dbos import DBOS

from ..common import RetryPolicy
from . import activities as activities_mod
from . import registry
from .payloads import deserialize_retry_policy

logger = logging.getLogger("temporal_dbos.activity_workflow")

ACTIVITY_DISPATCH_NAME = "__temporal_activity"

_activity_dispatcher_registered = False
_started_at_step: Optional[Callable[[], Any]] = None


def _activity_started_at() -> Any:
    """A step that records the activity workflow's start wall-clock, once, so
    the retry loop measures schedule-to-close elapsed deterministically (the
    recorded value replays identically — no live clock read in the loop)."""
    global _started_at_step
    if _started_at_step is None:

        @DBOS.step(name="__tdb_activity_started_at")
        async def started_at() -> float:
            return time_mod.time()

        _started_at_step = started_at
    return _started_at_step()


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
    # ``attempt_step_for`` raises a clear KeyError if the activity type is not
    # registered with *this* worker — the correct failure for a task_queue
    # pointed at a worker that doesn't host the activity.
    step_fn = activities_mod.attempt_step_for(activity_name)
    started_at = await _activity_started_at()
    attempt = int(meta.get("attempt", 1))
    while True:
        meta["attempt"] = attempt
        envelope: Dict[str, Any] = await step_fn(args, start_to_close, meta)
        if envelope.get("ok") or envelope.get("async_pending"):
            return envelope
        failure = envelope["failure"]
        if failure.get("cls") == "CancelledError":
            # Cancellation is terminal — a cancelled activity is never retried
            # (Temporal semantics). The interpreter resolves the awaiter as
            # cancelled (and confirms a WAIT_CANCELLATION_COMPLETED unwind).
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
        attempt += 1


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

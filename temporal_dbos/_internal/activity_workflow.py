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
from typing import Any, Dict

from dbos import DBOS

from . import activities as activities_mod
from . import registry

logger = logging.getLogger("temporal_dbos.activity_workflow")

ACTIVITY_DISPATCH_NAME = "__temporal_activity"

_activity_dispatcher_registered = False


async def _run_queued_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Run one attempt of a queued activity and return its envelope.

    The payload mirrors the interpreter's local-path attempt call
    (``_launch_attempt``): the activity type, its already-encoded args, the
    per-attempt ``start_to_close``, and the ``meta`` dict that backs
    ``activity.info()`` on this worker. The single-attempt step
    (``activities._make_attempt_step``) already returns a fully-formed envelope
    — ``{"ok", "result"|"failure", "ended_at", ...}`` — with user exceptions
    serialized inside it, so this workflow never raises on a user failure; it
    just returns the envelope as its DBOS output.
    """
    activity_name = payload["activity_name"]
    args = payload["args"]
    start_to_close = payload.get("start_to_close")
    meta = payload["meta"]
    # ``attempt_step_for`` raises a clear KeyError if the activity type is not
    # registered with *this* worker — the correct failure for a task_queue
    # pointed at a worker that doesn't host the activity.
    step_fn = activities_mod.attempt_step_for(activity_name)
    envelope: Dict[str, Any] = await step_fn(args, start_to_close, meta)
    return envelope


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

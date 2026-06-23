"""Shared in-workflow run enqueue.

Every durable chain hop starts one run under a deterministic id with the same
DBOS context plumbing: continue-as-new (``interpreter._begin_continue_as_new``),
child-workflow start (``interpreter._start_child``), and the dispatcher's
retry/cron continuation (``dispatcher._enqueue_next_run``). The per-run-timeout
re-application is correctness-sensitive (see ``enqueue_run``), so the plumbing
lives in one place rather than being copied per hop.
"""

from contextlib import nullcontext
from typing import Any, Callable, ContextManager, Optional

from dbos import (
    DBOS,
    SetEnqueueOptions,
    SetWorkflowAttributes,
    SetWorkflowID,
    SetWorkflowTimeout,
)


async def enqueue_run(
    dispatch_fn: Callable[..., Any],
    payload: Any,
    *,
    run_id: str,
    queue: Optional[Any],
    run_timeout: Optional[float] = None,
    attributes: Optional[Any] = None,
    delay_seconds: float = 0.0,
) -> None:
    """Durably start one run of ``dispatch_fn`` under ``run_id``.

    DBOS records in-workflow starts and ``SetWorkflowID`` re-attaches
    idempotently, so a crash anywhere after this replays into a re-attach
    rather than a twin run.

    ``run_timeout`` (seconds) is re-applied explicitly because an in-workflow
    start with no explicit timeout otherwise inherits the *closing* run's
    *absolute* deadline (DBOS derives an in-workflow child's deadline from the
    enclosing run when none is set), which would let a backed-off retry be born
    already expired; an explicit timeout on an
    enqueued workflow is converted to a deadline at dequeue (Temporal's per-run
    semantics). ``attributes`` is the encoded memo/search-attribute column for
    ``describe()``/visibility. ``delay_seconds`` (retry backoff / cron spacing)
    applies only on the queue-dispatched path; the in-process path ignores it.
    """
    timeout_ctx: ContextManager[Any] = (
        SetWorkflowTimeout(run_timeout) if run_timeout is not None else nullcontext()
    )
    attrs_ctx: ContextManager[Any] = (
        SetWorkflowAttributes(attributes) if attributes is not None else nullcontext()
    )
    delay_ctx: ContextManager[Any] = (
        SetEnqueueOptions(delay_seconds=delay_seconds)
        if delay_seconds > 0
        else nullcontext()
    )
    with SetWorkflowID(run_id), timeout_ctx, delay_ctx, attrs_ctx:
        if queue is not None:
            await queue.enqueue_async(dispatch_fn, payload)
        else:
            # Not queue-dispatched: start it directly in-process. Enqueue delays
            # don't apply on this path.
            await DBOS.start_workflow_async(dispatch_fn, payload)

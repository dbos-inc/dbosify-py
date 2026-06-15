"""Per-type DBOS workflow dispatchers and the workflow-task retry loop.

Each registered Temporal workflow type gets its own thin DBOS workflow named
``wf:{type}`` (resolved decision §10.1) that delegates to the interpreter.
Per-type registration keeps DBOS-native listing/filtering by name working
and lets clients enqueue by name without importing user code.

Workflow-task failure semantics (§4.2): a non-failure exception from
workflow code does NOT fail the workflow. The dispatcher logs loudly, sleeps
with capped backoff (a real sleep — this is outside the deterministic
boundary), rewinds the DBOS checkpoint cursor (``ctx.function_id``) to where
the interpreter started, and re-runs it — which replays from checkpoints,
exactly like crash recovery. The workflow stays PENDING (Temporal: RUNNING),
so users can fix the bug, redeploy, and the workflow resumes. Set
``TEMPORAL_DBOS_FAIL_FAST=1`` to fail immediately instead (dev/test).

This module also carries the Phase 0 in-process client helpers (start /
signal / update / query). The real ``Client`` facade replaces them in
Phase 1.
"""

import asyncio
import copy
import dataclasses
import logging
import os
import time as time_mod
import uuid
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine, Dict, List, Optional, Sequence, Type, Union

from dbos import (
    DBOS,
    SetEnqueueOptions,
    SetWorkflowID,
    SetWorkflowTimeout,
    WorkflowHandle,
)
from dbos._context import get_local_dbos_context  # see docs/phase0.md

from .. import exceptions

# Re-exported here for the Phase 0 helper API; the canonical home mirrors
# temporalio.client.WorkflowUpdateFailedError.
from ..client import WorkflowUpdateFailedError as WorkflowUpdateFailedError
from . import activities as activities_mod
from . import conversion, ids, inbox, registry, schedules
from .interpreter import (
    Interpreter,
    WorkflowCancelled,
    WorkflowContinuedAsNew,
    WorkflowTaskFailure,
    _safe_status,
)
from .payloads import (
    FailureEnvelope,
    RunMeta,
    SerializedContinueAsNew,
    SerializedWorkflowCancellation,
    SerializedWorkflowFailure,
    deserialize_failure,
    serialize_failure,
    unwrap_input,
    wrap_input,
)

logger = logging.getLogger("temporal_dbos.dispatcher")

FAIL_FAST_ENV = "TEMPORAL_DBOS_FAIL_FAST"
TASK_RETRY_INITIAL_ENV = "TEMPORAL_DBOS_TASK_RETRY_INITIAL_SECONDS"
TASK_RETRY_MAX_SECONDS = 60.0


def _reset_for_tests() -> None:
    """Clear all per-process temporal-dbos state. Test-only: needed when the
    DBOS registry is destroyed and re-created, which strands every function
    decorated against the old one.
    """
    from . import interpreter

    registry._dbos_workflows.clear()
    registry._workflows.clear()
    registry._activities.clear()
    registry.worker_failure_exception_types = ()
    activities_mod._attempt_steps.clear()
    interpreter._init_step = None
    interpreter._child_result_step = None
    interpreter._child_exists_step = None
    interpreter._update_validate_step = None
    interpreter._safe_status_step = None
    interpreter._safe_status_list_step = None


def register_worker(
    *,
    workflows: Sequence[Type[Any]] = (),
    activities: Sequence[Callable[..., Any]] = (),
    failure_exception_types: Sequence[Type[BaseException]] = (),
) -> None:
    """Register workflow classes and activity functions with this process.

    Must run before ``DBOS.launch()`` so recovery can resolve the per-type
    dispatchers. Re-registering a workflow type replaces its implementation
    — in-flight executions pick it up on their next workflow-task retry.
    """
    if failure_exception_types:
        registry.add_worker_failure_exception_types(failure_exception_types)
    for cls in workflows:
        defn = registry.workflow_definition_of(cls)
        registry.register_workflow(defn)
        if defn.name not in registry._dbos_workflows:
            registry.register_dbos_workflow(defn.name, _make_dbos_workflow(defn.name))
    for fn in activities:
        activity_defn = registry.activity_definition_of(fn)
        if activity_defn.fn is not fn:
            # A bound method: the definition was built at decoration time on
            # the unbound function; execute the bound callable the user
            # actually registered (temporalio supports method activities).
            activity_defn = dataclasses.replace(activity_defn, fn=fn)
        registry.register_activity(activity_defn)
        activities_mod.ensure_attempt_step(activity_defn.name)


def _make_dbos_workflow(
    type_name: str,
) -> Callable[[Any], Coroutine[Any, Any, Any]]:
    async def dispatch(payload: Any) -> Any:
        args, meta = unwrap_input(payload)
        # Chain hops (cron continuation, workflow retries) re-enqueue this
        # run's arguments at close; snapshot them before user code can
        # mutate nested structures in place.
        hops_possible = meta.cron is not None or meta.retry_policy is not None
        hop_args = copy.deepcopy(args) if hops_possible else None
        run_flags = {"cancel_observed": False}
        try:
            result = await _run_workflow_task_loop(type_name, args, meta, run_flags)
        except WorkflowContinuedAsNew as can:
            # The chain-hop marker: the next run is already enqueued; this
            # run's status maps to CONTINUED_AS_NEW and awaiters follow
            # envelope["new_run_id"].
            raise SerializedContinueAsNew({"new_run_id": can.new_run_id}) from None
        except WorkflowCancelled as cancelled:
            # The _TemporalCancelledMarker: cooperative cancellation maps to
            # status CANCELED (§6.2), distinct from FAILED below and from
            # TERMINATED (native DBOS cancel, no record at all). Cancellation
            # ends the chain: no retry, no cron continuation (Temporal
            # semantics).
            raise SerializedWorkflowCancellation(
                serialize_failure(cancelled.cause)
            ) from None
        except exceptions.FailureError as err:
            # Record workflow failures in the stable envelope format so
            # clients reconstruct the exact exception, cause chain included
            # (pickle would drop __cause__).
            envelope = serialize_failure(err)
            next_run_id = await _continue_chain_after_failure(
                type_name, hop_args, meta, envelope, run_flags["cancel_observed"]
            )
            if next_run_id is not None:
                # result(follow_runs=True) follows a failed run to its
                # retry/cron successor, exactly as temporalio follows
                # new_execution_run_id on a failure event.
                envelope["new_run_id"] = next_run_id
            raise SerializedWorkflowFailure(envelope) from None
        if meta.cron is not None:
            assert hop_args is not None
            carryover = await _drain_unconsumed_inbox()
            # A cancel requested around the run's close — observed but
            # swallowed by user code, or still sitting unconsumed in the
            # inbox of a run that never parked — ends the cron chain
            # (Temporal suppresses cron continuation once cancellation is
            # requested). The run itself still closes COMPLETED.
            if not run_flags["cancel_observed"] and not _contains_cancel(carryover):
                next_meta = meta.carried_forward()
                # Encoded (no codec — read back synchronously by
                # get_last_completion_result, like query results).
                next_meta.last_completion = {
                    "value": conversion.encode_value_sync(result)
                }
                next_meta.last_failure = None
                new_run_id = await _enqueue_next_run(
                    type_name,
                    hop_args,
                    next_meta,
                    delay_seconds=schedules.next_fire_delay(
                        meta.cron, datetime.now(timezone.utc)
                    ),
                )
                await _forward_carryover(new_run_id, carryover)
        # Encode the result for the DBOS output: the client (and any awaiting
        # parent) decodes it against the run's result type. (The cron
        # last_completion above stays raw for now — Stage 3.)
        return await conversion.encode_value(result)

    dispatch.__name__ = dispatch.__qualname__ = f"wf:{type_name}"
    decorated: Callable[[Any], Coroutine[Any, Any, Any]] = DBOS.workflow(
        name=f"wf:{type_name}"
    )(dispatch)
    return decorated


async def _continue_chain_after_failure(
    type_name: str,
    hop_args: Optional[List[Any]],
    meta: RunMeta,
    envelope: FailureEnvelope,
    cancel_observed: bool,
) -> Optional[str]:
    """Start the chain's next run after a workflow failure, if anything
    calls for one: a retry policy with attempts left wins (backoff delay,
    attempt+1); otherwise a cron chain continues at its next occurrence
    (attempt resets, as in Temporal). A cancel requested around the close
    ends the chain either way. Returns the new run id, or None when the
    failure is terminal for the chain.
    """
    if hop_args is None:
        return None
    carryover = await _drain_unconsumed_inbox()
    if cancel_observed or _contains_cancel(carryover):
        return None
    new_run_id: Optional[str] = None
    if meta.retry_policy is not None:
        delay = _workflow_retry_delay(meta.retry_policy, meta.attempt, envelope)
        if delay is not None:
            next_meta = meta.carried_forward()
            next_meta.attempt = meta.attempt + 1
            next_meta.last_failure = dict(envelope)
            new_run_id = await _enqueue_next_run(
                type_name, hop_args, next_meta, delay_seconds=delay
            )
    if new_run_id is None and meta.cron is not None:
        next_meta = meta.carried_forward()
        next_meta.last_failure = dict(envelope)
        new_run_id = await _enqueue_next_run(
            type_name,
            hop_args,
            next_meta,
            delay_seconds=schedules.next_fire_delay(
                meta.cron, datetime.now(timezone.utc)
            ),
        )
    if new_run_id is not None:
        await _forward_carryover(new_run_id, carryover)
    return new_run_id


async def _drain_unconsumed_inbox() -> List[Any]:
    """Collect inbox messages still unconsumed at run close. Every recv(0)
    is checkpointed, so the drain replays identically on recovery."""
    messages: List[Any] = []
    while True:
        message = await DBOS.recv_async(inbox.INBOX_TOPIC, 0)
        if message is None:
            return messages
        messages.append(message)


def _contains_cancel(messages: List[Any]) -> bool:
    return any(
        isinstance(message, dict) and message.get("kind") == "cancel"
        for message in messages
    )


async def _forward_carryover(new_run_id: str, messages: List[Any]) -> None:
    """Forward unconsumed messages to the chain's next run (mirroring the
    interpreter's continue-as-new carryover): an id-addressed signal racing
    the close keeps reaching the chain instead of dying with this run.
    Activity envelopes are run-scoped (activity seqs reset per run) and die
    here, exactly as in the CAN path.
    """
    for message in messages:
        if isinstance(message, dict) and message.get("kind") in (
            "activity_result",
            "activity_heartbeat",
        ):
            continue
        await DBOS.send_async(new_run_id, message, inbox.INBOX_TOPIC)


def _workflow_retry_delay(
    policy: Dict[str, Any], attempt: int, failure: FailureEnvelope
) -> Optional[float]:
    """Backoff before the next workflow-retry attempt, or None to give up.

    Mirrors the activity retry decision (interpreter._retry_decision) minus
    the schedule_to_close bound: there is no overall workflow-retry deadline
    here (execution_timeout enforcement is a separate, unimplemented knob).
    """
    cls_name = failure["cls"]
    if cls_name in ("CancelledError", "TerminatedError"):
        # Never retried, regardless of policy (Temporal's isRetryable):
        # this covers a cancellation outcome nobody requested externally —
        # e.g. user code cancelling its own primary task — which reaches
        # here classified as a plain workflow failure. (A cron chain still
        # continues past such a failure, as Temporal's cron does; only
        # *requested* cancellation ends the chain.)
        return None
    if cls_name == "TimeoutError" and failure.get("timeout_type") not in (
        int(exceptions.TimeoutType.START_TO_CLOSE),
        int(exceptions.TimeoutType.HEARTBEAT),
    ):
        # Temporal retries only start-to-close and heartbeat timeouts.
        return None
    if failure.get("non_retryable"):
        return None
    failure_type = failure.get("type") or failure["cls"]
    if failure_type in set(policy.get("non_retryable_error_types") or ()):
        return None
    maximum_attempts = policy.get("maximum_attempts") or 0
    if maximum_attempts and attempt >= maximum_attempts:
        return None
    override = failure.get("next_retry_delay")
    if override is not None:
        return float(override)
    initial = float(policy["initial_interval"])
    delay = initial * float(policy["backoff_coefficient"]) ** (attempt - 1)
    maximum = policy.get("maximum_interval")
    return min(delay, float(maximum) if maximum is not None else initial * 100)


async def _enqueue_next_run(
    type_name: str, args: List[Any], meta: RunMeta, *, delay_seconds: float
) -> str:
    """Enqueue the chain's next run (workflow retry / cron continuation),
    mirroring the interpreter's continue-as-new enqueue: the run id is
    deterministic (current index + 1) and DBOS records in-workflow starts,
    so a crash anywhere after this replays into an idempotent re-attach.
    Replays skip the enqueue entirely, which is what makes the live clock
    reads behind ``delay_seconds`` replay-safe: the delay only ever takes
    effect once, at first execution.
    """
    ctx = get_local_dbos_context()
    assert ctx is not None, "chain hops must run inside a DBOS workflow"
    base, index = ids.parse_run(ctx.workflow_id)
    new_run_id = ids.run_dbos_id(base, index + 1)
    dispatch_fn = registry.dbos_workflow_for(type_name)
    # Safe (JSON-serializable) status read — a whole WorkflowStatus can't be
    # checkpointed by the JSON serializer (see interpreter._safe_status).
    fields = await _safe_status(ctx.workflow_id)
    queue_name = fields["queue_name"] if fields else None
    queue = (
        await DBOS.retrieve_queue_async(queue_name) if queue_name is not None else None
    )
    payload = wrap_input(args, meta)
    delay_ctx = (
        SetEnqueueOptions(delay_seconds=delay_seconds)
        if delay_seconds > 0
        else nullcontext()
    )
    # Re-apply the per-run timeout explicitly: an in-workflow start with no
    # explicit timeout inherits this (closing) run's *absolute* deadline
    # (dbos._core._get_timeout_deadline), which would let a backed-off
    # attempt be born already expired. An explicit timeout on an enqueued
    # workflow is converted to a deadline at dequeue — Temporal's per-run
    # semantics. (Runs whose start predates the meta-envelope still inherit;
    # acceptable for pre-envelope checkpoints.)
    timeout_ctx = (
        SetWorkflowTimeout(meta.run_timeout)
        if meta.run_timeout is not None
        else nullcontext()
    )
    with SetWorkflowID(new_run_id), timeout_ctx, delay_ctx:
        if queue is not None:
            await queue.enqueue_async(dispatch_fn, payload)
        else:
            # This run wasn't queue-dispatched (Phase 0 helpers): start the
            # next run directly in-process. Enqueue delays don't apply on
            # this path; queue-dispatched runs (every Client start) do.
            await DBOS.start_workflow_async(dispatch_fn, payload)
    return new_run_id


async def _run_workflow_task_loop(
    type_name: str,
    args: List[Any],
    meta: RunMeta,
    run_flags: Optional[Dict[str, bool]] = None,
) -> Any:
    ctx = get_local_dbos_context()
    assert ctx is not None, "dispatcher must run inside a DBOS workflow"
    start_function_id = ctx.function_id
    backoff = float(os.environ.get(TASK_RETRY_INITIAL_ENV, "1.0"))
    while True:
        # Looked up fresh each attempt so a replaced implementation takes
        # effect ("fix the bug, redeploy").
        defn = registry.lookup_workflow(type_name)
        interpreter = Interpreter(defn, args, meta)
        try:
            try:
                return await interpreter.execute()
            finally:
                # Whether the run returned, was cancelled, or failed, the
                # chain-hop decision needs to know a cancel request was
                # observed (replay-stable: it derives from checkpointed
                # inbox deliveries). Task-failure retries overwrite this on
                # their next attempt.
                if run_flags is not None:
                    run_flags["cancel_observed"] = interpreter._cancel_requested
        except WorkflowTaskFailure as failure:
            if os.environ.get(FAIL_FAST_ENV):
                # Bare raise: `from None` would clobber the user exception's
                # own __cause__ chain.
                raise failure.cause
            logger.error(
                "Workflow task failed for %s (workflow %s); workflow stays "
                "RUNNING, retrying in %.1fs. Fix the workflow code and "
                "redeploy (or set %s=1 in dev to fail fast).",
                type_name,
                ctx.workflow_id,
                backoff,
                FAIL_FAST_ENV,
                exc_info=failure.cause,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, TASK_RETRY_MAX_SECONDS)
            # Rewind the checkpoint cursor: the fresh interpreter re-runs
            # the same logical sequence and replays recorded results, the
            # same way crash recovery does.
            ctx.function_id = start_function_id


# ---------------------------------------------------------------------------
# Phase 0 in-process client helpers (superseded by the Client facade in
# Phase 1). Cross-process callers use DBOSClient with the inbox envelope
# helpers directly.
# ---------------------------------------------------------------------------


def _type_name(workflow: Union[Type[Any], str]) -> str:
    if isinstance(workflow, str):
        return workflow
    return registry.workflow_definition_of(workflow).name


def start_workflow(
    workflow: Union[Type[Any], str],
    args: Sequence[Any] = (),
    *,
    workflow_id: str,
) -> "WorkflowHandle[Any]":
    """Start a Temporal workflow; returns the underlying DBOS handle."""
    fn = registry.dbos_workflow_for(_type_name(workflow))
    with SetWorkflowID(workflow_id):
        return DBOS.start_workflow(fn, conversion.encode_values_sync(args))


def workflow_result(
    handle: "WorkflowHandle[Any]", type_hint: Optional[type] = None
) -> Any:
    """Decoded result for a Phase-0-started workflow (the raw DBOS handle's
    ``get_result`` returns the encoded payload dict). Failures propagate as the
    serialized markers, as before."""
    return conversion.decode_value_sync(handle.get_result(), type_hint)


def signal_workflow(
    workflow_id: str, signal_name: str, args: Sequence[Any] = ()
) -> None:
    DBOS.send(
        workflow_id,
        inbox.signal_envelope(signal_name, conversion.encode_values_sync(args)),
        inbox.INBOX_TOPIC,
    )


def execute_update(
    workflow_id: str,
    update_name: str,
    args: Sequence[Any] = (),
    *,
    update_id: Optional[str] = None,
    timeout_seconds: float = 60,
) -> Any:
    update_id = update_id or str(uuid.uuid4())
    DBOS.send(
        workflow_id,
        inbox.update_envelope(
            update_name, conversion.encode_values_sync(args), update_id
        ),
        inbox.INBOX_TOPIC,
        idempotency_key=update_id,
    )
    reply = DBOS.get_event(
        workflow_id, inbox.update_result_key(update_id), timeout_seconds
    )
    return _unwrap_reply(reply, kind="update", timeout_seconds=timeout_seconds)


def query_workflow(
    workflow_id: str,
    query_name: str,
    args: Sequence[Any] = (),
    *,
    timeout_seconds: float = 60,
) -> Any:
    request_id = str(uuid.uuid4())
    DBOS.send(
        workflow_id,
        inbox.query_envelope(
            query_name, conversion.encode_values_sync(args), request_id
        ),
        inbox.INBOX_TOPIC,
    )
    reply = DBOS.get_event(
        workflow_id, inbox.query_result_key(request_id), timeout_seconds
    )
    return _unwrap_reply(reply, kind="query", timeout_seconds=timeout_seconds)


def _unwrap_reply(reply: Any, *, kind: str, timeout_seconds: float) -> Any:
    if reply is None:
        raise TimeoutError(f"{kind} did not complete within {timeout_seconds}s")
    if reply["status"] == "completed":
        return conversion.decode_value_sync(reply["result"])
    raise WorkflowUpdateFailedError(deserialize_failure(reply["failure"]))


def workflow_status(workflow_id: str) -> Optional[str]:
    status = DBOS.get_workflow_status(workflow_id)
    return status.status if status is not None else None


def wait_for_workflow_status(
    workflow_id: str, expected: str, *, timeout_seconds: float = 10
) -> None:
    deadline = time_mod.monotonic() + timeout_seconds
    while time_mod.monotonic() < deadline:
        if workflow_status(workflow_id) == expected:
            return
        time_mod.sleep(0.05)
    raise TimeoutError(
        f"workflow {workflow_id} never reached status {expected}; "
        f"currently {workflow_status(workflow_id)}"
    )

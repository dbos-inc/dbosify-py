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
import dataclasses
import logging
import os
import time as time_mod
import uuid
from typing import Any, Callable, Coroutine, Dict, List, Optional, Sequence, Type, Union

from dbos import DBOS, SetWorkflowID, WorkflowHandle
from dbos._context import get_local_dbos_context  # see docs/phase0.md

from .. import exceptions

# Re-exported here for the Phase 0 helper API; the canonical home mirrors
# temporalio.client.WorkflowUpdateFailedError.
from ..client import WorkflowUpdateFailedError as WorkflowUpdateFailedError
from . import activities as activities_mod
from . import inbox, registry
from .interpreter import Interpreter, WorkflowTaskFailure
from .payloads import SerializedWorkflowFailure, deserialize_failure, serialize_failure

logger = logging.getLogger("temporal_dbos.dispatcher")

FAIL_FAST_ENV = "TEMPORAL_DBOS_FAIL_FAST"
TASK_RETRY_INITIAL_ENV = "TEMPORAL_DBOS_TASK_RETRY_INITIAL_SECONDS"
TASK_RETRY_MAX_SECONDS = 60.0

_dbos_workflows: Dict[str, Callable[[List[Any]], Coroutine[Any, Any, Any]]] = {}


def _reset_for_tests() -> None:
    """Clear all per-process temporal-dbos state. Test-only: needed when the
    DBOS registry is destroyed and re-created, which strands every function
    decorated against the old one.
    """
    from . import interpreter

    _dbos_workflows.clear()
    registry._workflows.clear()
    registry._activities.clear()
    registry.worker_failure_exception_types = ()
    activities_mod._attempt_steps.clear()
    interpreter._init_step = None


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
        if defn.name not in _dbos_workflows:
            _dbos_workflows[defn.name] = _make_dbos_workflow(defn.name)
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
) -> Callable[[List[Any]], Coroutine[Any, Any, Any]]:
    async def dispatch(args: List[Any]) -> Any:
        try:
            return await _run_workflow_task_loop(type_name, args)
        except exceptions.FailureError as err:
            # Record workflow failures in the stable envelope format so
            # clients reconstruct the exact exception, cause chain included
            # (pickle would drop __cause__).
            raise SerializedWorkflowFailure(serialize_failure(err)) from None

    dispatch.__name__ = dispatch.__qualname__ = f"wf:{type_name}"
    decorated: Callable[[List[Any]], Coroutine[Any, Any, Any]] = DBOS.workflow(
        name=f"wf:{type_name}"
    )(dispatch)
    return decorated


async def _run_workflow_task_loop(type_name: str, args: List[Any]) -> Any:
    ctx = get_local_dbos_context()
    assert ctx is not None, "dispatcher must run inside a DBOS workflow"
    start_function_id = ctx.function_id
    backoff = float(os.environ.get(TASK_RETRY_INITIAL_ENV, "1.0"))
    while True:
        # Looked up fresh each attempt so a replaced implementation takes
        # effect ("fix the bug, redeploy").
        defn = registry.lookup_workflow(type_name)
        try:
            return await Interpreter(defn, args).execute()
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
    fn = _dbos_workflows.get(_type_name(workflow))
    if fn is None:
        raise KeyError(f"Workflow type {_type_name(workflow)!r} is not registered")
    with SetWorkflowID(workflow_id):
        return DBOS.start_workflow(fn, list(args))


def signal_workflow(
    workflow_id: str, signal_name: str, args: Sequence[Any] = ()
) -> None:
    DBOS.send(workflow_id, inbox.signal_envelope(signal_name, args), inbox.INBOX_TOPIC)


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
        inbox.update_envelope(update_name, args, update_id),
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
        inbox.query_envelope(query_name, args, request_id),
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
        return reply["result"]
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

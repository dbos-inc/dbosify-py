"""Client for starting and interacting with workflows, mirroring
``temporalio.client`` in shape while taking DBOS machinery directly: a
``Client`` wraps a ``dbos.DBOSClient`` (which carries the database URL and
system schema), rather than parsing a Temporal-style target host.

Surface: ``start_workflow`` / ``execute_workflow`` / ``get_workflow_handle``,
and ``WorkflowHandle`` with ``result``/``signal``/``query``/``execute_update``/
``describe``/``cancel`` (cooperative, §6.5) / ``terminate`` (forceful).
Parameters not yet honored are accepted and ignored with a debug log.
"""

import asyncio
import json
import logging
import os
import uuid as uuid_mod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import IntEnum
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Type, Union

from dbos import DBOSClient, EnqueueOptions, WorkflowStatus
from dbos._error import DBOSAwaitedWorkflowCancelledError

from . import exceptions
from ._internal import ids, inbox
from ._internal import registry as _registry
from ._internal import status as _status
from ._internal.payloads import (
    SerializedContinueAsNew,
    SerializedWorkflowFailure,
    deserialize_failure,
    serialize_failure,
)
from ._internal.status import WorkflowExecutionStatus
from .common import (
    QueryRejectCondition,
    RetryPolicy,
    WorkflowIDConflictPolicy,
    WorkflowIDReusePolicy,
)
from .workflow import _UpdateMethod

# Worst-case latency for client-side get_event when a LISTEN/NOTIFY wakeup is
# missed (see docs/phase0.md); DBOSClient has no public knob yet.
CLIENT_POLL_ENV = "TEMPORAL_DBOS_CLIENT_POLL_SECONDS"
DEFAULT_CLIENT_POLL_SECONDS = 1.0

# How often reply waits (update acceptance/result, query replies) re-check
# newer runs of the chain: a message still unconsumed when its target run
# continues-as-new is forwarded to (and answered under) a later run's id.
REPLY_SWEEP_INTERVAL_SECONDS = 1.0

__all__ = [
    "AsyncActivityCancelledError",
    "AsyncActivityHandle",
    "Client",
    "WithStartWorkflowOperation",
    "WorkflowContinuedAsNewError",
    "WorkflowQueryRejectedError",
    "WorkflowHandle",
    "WorkflowExecution",
    "WorkflowExecutionDescription",
    "WorkflowExecutionStatus",
    "WorkflowFailureError",
    "WorkflowQueryFailedError",
    "WorkflowUpdateFailedError",
    "WorkflowUpdateHandle",
    "WorkflowUpdateStage",
]

logger = logging.getLogger("temporal_dbos.client")

_arg_unset = object()


class WorkflowFailureError(exceptions.TemporalError):
    """The workflow run did not complete successfully; ``cause`` is the
    exact reconstructed failure.
    """

    def __init__(self, *, cause: BaseException) -> None:
        super().__init__("Workflow execution failed")
        self.__cause__ = cause


class WorkflowContinuedAsNewError(exceptions.TemporalError):
    """The workflow continued as new while waiting with
    ``follow_runs=False``; ``new_execution_run_id`` is the next run."""

    def __init__(self, new_execution_run_id: str) -> None:
        super().__init__("Workflow continued as new")
        self._new_execution_run_id = new_execution_run_id

    @property
    def new_execution_run_id(self) -> str:
        """The run id of the next run in the chain."""
        return self._new_execution_run_id


class WorkflowQueryRejectedError(exceptions.TemporalError):
    """The query was rejected by its ``reject_condition``: the workflow's
    status matched the condition before the query was sent."""

    def __init__(self, status: Optional["WorkflowExecutionStatus"]) -> None:
        super().__init__(f"Query rejected, status: {status}")
        self._status = status

    @property
    def status(self) -> Optional["WorkflowExecutionStatus"]:
        """The workflow execution status that caused the rejection."""
        return self._status


class WorkflowQueryFailedError(exceptions.TemporalError):
    """The query handler failed or was not found."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class WorkflowUpdateFailedError(exceptions.TemporalError):
    """The update was rejected by its validator or failed in its handler."""

    def __init__(self, cause: BaseException) -> None:
        super().__init__("Workflow update failed")
        self.__cause__ = cause


class WorkflowUpdateStage(IntEnum):
    """Stage to wait for in ``start_update``, mirroring
    ``temporalio.client.WorkflowUpdateStage``. ADMITTED is not supported
    (same as temporalio).
    """

    ADMITTED = 1
    ACCEPTED = 2
    COMPLETED = 3


class WorkflowUpdateHandle:
    """Handle for a workflow update: poll its result with
    :py:meth:`result`. Obtained from ``start_update``/``execute_update``.
    """

    def __init__(
        self,
        client: "Client",
        id: str,
        workflow_id: str,
        *,
        workflow_run_id: Optional[str] = None,
        result_type: Optional[type] = None,
        known_outcome: Optional[Any] = None,
    ) -> None:
        self._client = client
        self._id = id
        self._workflow_id = workflow_id
        self._workflow_run_id = workflow_run_id
        self._known_outcome = known_outcome  # result_type unused (pickle)

    @property
    def id(self) -> str:
        """ID of this update request."""
        return self._id

    @property
    def workflow_id(self) -> str:
        """The ID of the workflow targeted by this update."""
        return self._workflow_id

    @property
    def workflow_run_id(self) -> Optional[str]:
        """The run targeted by this update, if known."""
        return self._workflow_run_id

    async def result(
        self,
        *,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any:
        """Wait for and return the update's result; raises
        :py:class:`WorkflowUpdateFailedError` on rejection or failure.
        """
        _ignore_rpc_options("update result", rpc_metadata, None)
        outcome = self._known_outcome
        if outcome is None:
            timeout = rpc_timeout.total_seconds() if rpc_timeout else 60.0
            outcome = await self._client._await_reply_event(
                self._workflow_id,
                self._workflow_run_id or self._workflow_id,
                inbox.update_result_key(self._id),
                timeout,
            )
            if outcome is None:
                raise TimeoutError(f"update did not complete within {timeout}s")
            self._known_outcome = outcome
        if outcome["status"] == "completed":
            return outcome["result"]
        raise WorkflowUpdateFailedError(deserialize_failure(outcome["failure"]))


class AsyncActivityCancelledError(exceptions.TemporalError):
    """The async activity was cancelled (or its run closed): further
    completion attempts are pointless. Raised from the handle's
    heartbeat/complete/fail once the workflow side marks the activity gone.
    """

    def __init__(self, details: Optional[Any] = None) -> None:
        super().__init__("Activity cancelled")
        self.details = details


class AsyncActivityHandle:
    """Handle to an activity completing asynchronously
    (``activity.raise_complete_async()``), addressed by its task token or by
    a (workflow_id, run_id, activity_id) reference. Operations deliver
    checkpointed inbox envelopes to the activity's run.
    """

    def __init__(self, client: "Client", id_or_token: Any) -> None:
        self._client = client
        self._workflow_id: Optional[str] = None
        self._run_id: Optional[str] = None
        if isinstance(id_or_token, bytes):
            token = json.loads(id_or_token.decode())
            self._run_id = token["run"]
            self._activity_id: str = token["aid"]
        else:
            workflow_id, run_id, activity_id = id_or_token
            if not workflow_id or not activity_id:
                raise ValueError(
                    "reference addressing requires workflow_id and activity_id"
                )
            self._workflow_id = workflow_id
            self._run_id = run_id
            self._activity_id = activity_id

    async def _target(self) -> str:
        if self._run_id is not None:
            return self._run_id
        assert self._workflow_id is not None
        resolved = await self._client._current_run(self._workflow_id)
        if resolved is None:
            raise RuntimeError(f"Workflow not found: {self._workflow_id!r}")
        return resolved[1].workflow_id

    async def _send(self, envelope: Dict[str, Any]) -> None:
        target = await self._target()
        await self._send_checked(target, envelope)

    async def _send_checked(self, target: str, envelope: Dict[str, Any]) -> None:
        # The gone-event is set (checkpointed) when the activity is
        # cancelled or its run closes while parked: raise instead of
        # delivering into the void.
        gone = await self._client._dbos_client.get_event_async(
            target, inbox.async_activity_gone_key(self._activity_id), 0
        )
        if gone:
            raise AsyncActivityCancelledError()
        await self._client._dbos_client.send_async(target, envelope, inbox.INBOX_TOPIC)

    async def complete(
        self,
        result: Optional[Any] = _arg_unset,
        *,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Complete the activity with a result."""
        _ignore_rpc_options("async activity complete", rpc_metadata, rpc_timeout)
        await self._send(
            inbox.activity_result_envelope(
                self._activity_id, result=None if result is _arg_unset else result
            )
        )

    async def fail(
        self,
        error: Exception,
        *,
        last_heartbeat_details: Sequence[Any] = [],
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Fail the activity; the activity's retry policy applies (a
        retryable failure schedules another attempt, re-running the
        function)."""
        _ignore_rpc_options("async activity fail", rpc_metadata, rpc_timeout)
        await self._send(
            inbox.activity_result_envelope(
                self._activity_id, failure=serialize_failure(error)
            )
        )

    async def heartbeat(
        self,
        *details: Any,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Send a heartbeat for the activity."""
        _ignore_rpc_options("async activity heartbeat", rpc_metadata, rpc_timeout)
        await self._send(
            inbox.activity_heartbeat_envelope(self._activity_id, list(details))
        )

    async def report_cancellation(
        self,
        *details: Any,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Report the activity as cancelled. Never raises on an
        already-gone activity: this call IS the acknowledgment in the
        canonical completer pattern (heartbeat raises
        AsyncActivityCancelledError -> report_cancellation confirms).
        """
        _ignore_rpc_options(
            "async activity report_cancellation", rpc_metadata, rpc_timeout
        )
        await self._client._dbos_client.send_async(
            await self._target(),
            inbox.activity_result_envelope(self._activity_id, cancelled=True),
            inbox.INBOX_TOPIC,
        )


class WithStartWorkflowOperation:
    """Defines the workflow-start half of an update-with-start request
    (mirroring ``temporalio.client.WithStartWorkflowOperation``): start the
    workflow per ``id_conflict_policy`` (typically USE_EXISTING — "create
    the cart if it doesn't exist") and deliver the update to it. The
    workflow handle is available via :py:meth:`workflow_handle` even if the
    update itself fails. Single-use.
    """

    def __init__(
        self,
        workflow: Any,
        arg: Any = _arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        id_conflict_policy: WorkflowIDConflictPolicy,
        result_type: Optional[type] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: WorkflowIDReusePolicy = WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        retry_policy: Optional[RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[Any] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
        priority: Optional[Any] = None,
    ) -> None:
        # Required (no default), matching temporalio; explicit UNSPECIFIED
        # is also rejected.
        if id_conflict_policy == WorkflowIDConflictPolicy.UNSPECIFIED:
            raise ValueError("WithStartWorkflowOperation requires id_conflict_policy")
        self._start_kwargs: Dict[str, Any] = dict(
            args=_resolve_args(arg, args),
            id=id,
            task_queue=task_queue,
            id_conflict_policy=id_conflict_policy,
            result_type=result_type,
            execution_timeout=execution_timeout,
            run_timeout=run_timeout,
            task_timeout=task_timeout,
            id_reuse_policy=id_reuse_policy,
            retry_policy=retry_policy,
            cron_schedule=cron_schedule,
            memo=memo,
            search_attributes=search_attributes,
            static_summary=static_summary,
            static_details=static_details,
            start_delay=start_delay,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
            priority=priority,
        )
        self._workflow = workflow
        self._handle: Optional["WorkflowHandle"] = None
        self._used = False

    async def workflow_handle(self) -> "WorkflowHandle":
        """The handle for the started (or attached-to) workflow. Available
        once the operation has been used, even if the update failed."""
        if self._handle is None:
            raise RuntimeError(
                "WithStartWorkflowOperation has not been used in an "
                "update-with-start call yet"
            )
        return self._handle


@dataclass(frozen=True)
class WorkflowExecution:
    """Info for a single workflow execution run (Phase 1 subset of
    temporalio's; field order matches theirs). Constructed by the SDK,
    never by users.
    """

    close_time: Optional[datetime] = None
    id: str = ""
    parent_id: Optional[str] = None
    run_id: str = ""
    start_time: Optional[datetime] = None
    status: Optional[WorkflowExecutionStatus] = None
    task_queue: Optional[str] = None
    workflow_type: str = ""


@dataclass(frozen=True)
class WorkflowExecutionDescription(WorkflowExecution):
    """Description for a single workflow execution run."""


def _workflow_type_name(workflow: Any) -> str:
    """Resolve a workflow reference the temporalio ways: the class, the run
    method (``MyWorkflow.run``), or the type-name string.
    """
    if isinstance(workflow, str):
        return workflow
    if isinstance(workflow, type):
        return _registry.workflow_definition_of(workflow).name
    name = getattr(workflow, _registry.WORKFLOW_NAME_ATTR, None)
    if name is not None:
        return str(name)
    raise TypeError(
        f"Cannot resolve a workflow type from {workflow!r}: pass the "
        "@workflow.defn class, its @workflow.run method, or the type name"
    )


def _signal_name(signal: Any) -> str:
    if isinstance(signal, str):
        return signal
    name = getattr(signal, _registry.SIGNAL_ATTR, None)
    if name is None:
        raise TypeError(f"{signal!r} is not a @workflow.signal method or name")
    return str(name)


def _query_name(query: Any) -> str:
    if isinstance(query, str):
        return query
    name = getattr(query, _registry.QUERY_ATTR, None)
    if name is None:
        raise TypeError(f"{query!r} is not a @workflow.query method or name")
    return str(name)


def _update_name(update: Any) -> str:
    if isinstance(update, str):
        return update
    if isinstance(update, _UpdateMethod):
        return update.name
    raise TypeError(f"{update!r} is not a @workflow.update method or name")


def _resolve_args(arg: Any, args: Sequence[Any]) -> List[Any]:
    if arg is not _arg_unset:
        if args:
            raise ValueError("Cannot have both arg and args")
        return [arg]
    return list(args)


def _to_datetime(epoch_ms: Optional[int]) -> Optional[datetime]:
    if epoch_ms is None:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)


def _ignore_rpc_options(
    where: str, rpc_metadata: Mapping[str, Any], rpc_timeout: Optional[timedelta]
) -> None:
    """RPC transport options have no temporal-dbos equivalent; accept and
    debug-log them (DESIGN convention for tuning parameters)."""
    if rpc_metadata:
        logger.debug("%s: ignoring rpc_metadata", where)
    if rpc_timeout is not None:
        logger.debug("%s: ignoring rpc_timeout", where)


class Client:
    """Client for accessing temporal-dbos, wrapping a ``dbos.DBOSClient``.

    The DBOSClient carries the connection (database URL, system schema), so
    namespacing rides on its ``dbos_system_schema``. Construct directly or
    via the async :py:meth:`connect` (kept for temporalio shape).
    """

    def __init__(
        self,
        dbos_client: DBOSClient,
        *,
        default_workflow_query_reject_condition: Optional[QueryRejectCondition] = None,
    ) -> None:
        self._dbos_client = dbos_client
        self._default_query_reject_condition = default_workflow_query_reject_condition
        # Bound the LISTEN/NOTIFY-miss latency for get_event-based replies
        # (updates/queries). Private until DBOS exposes an option.
        self._dbos_client._sys_db._notification_fallback_polling_interval = float(
            os.environ.get(CLIENT_POLL_ENV, str(DEFAULT_CLIENT_POLL_SECONDS))
        )

    @classmethod
    async def connect(
        cls,
        dbos_client: DBOSClient,
        *,
        default_workflow_query_reject_condition: Optional[QueryRejectCondition] = None,
    ) -> "Client":
        """Create a client from a ``dbos.DBOSClient``."""
        return cls(
            dbos_client,
            default_workflow_query_reject_condition=default_workflow_query_reject_condition,
        )

    # ------------------------------------------------------------------
    # Workflow start
    # ------------------------------------------------------------------

    async def start_workflow(
        self,
        workflow: Any,
        arg: Any = _arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: Optional[type] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: WorkflowIDReusePolicy = WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: WorkflowIDConflictPolicy = WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[Any] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: Optional[Any] = None,
        request_id: Optional[str] = None,
        **unsupported: Any,
    ) -> "WorkflowHandle":
        """Start a workflow and return its handle.

        Phase 1 honors arg/args, id, task_queue, run_timeout, the
        USE_EXISTING/FAIL conflict policies, the ALLOW_DUPLICATE /
        ALLOW_DUPLICATE_FAILED_ONLY / REJECT_DUPLICATE reuse policies,
        start_delay, and start_signal. Workflow retry_policy and
        cron_schedule are Phase 3.
        """
        for key, value in {
            "result_type": result_type,
            "execution_timeout": execution_timeout,
            "task_timeout": task_timeout,
            "retry_policy": retry_policy,
            "cron_schedule": cron_schedule or None,
            "memo": memo,
            "search_attributes": search_attributes,
            "static_summary": static_summary,
            "static_details": static_details,
            "rpc_metadata": rpc_metadata or None,
            "rpc_timeout": rpc_timeout,
            "request_eager_start": request_eager_start or None,
            "priority": priority,
            "request_id": request_id,
            **unsupported,
        }.items():
            if value is not None:
                logger.debug("start_workflow: ignoring unsupported option %r", key)

        if not task_queue or not isinstance(task_queue, str):
            # Without this, a None/empty queue name would enqueue a workflow
            # no worker can ever dequeue — a silent black hole.
            raise ValueError("task_queue must be a non-empty string")
        type_name = _workflow_type_name(workflow)
        workflow_args = _resolve_args(arg, args)
        ids.validate_workflow_id(id)

        current = await self._current_run(id)
        run_index = 0
        if current is not None:
            current_index, current_status = current
            if _status.is_open(current_status.status):
                # Conflict policies (vs a RUNNING run). There is an inherent
                # TOCTOU window here, accepted for v1 (DESIGN §6.4).
                if id_conflict_policy == WorkflowIDConflictPolicy.USE_EXISTING:
                    return WorkflowHandle(self, id, run_id=current_status.workflow_id)
                if (
                    id_conflict_policy == WorkflowIDConflictPolicy.TERMINATE_EXISTING
                    or id_reuse_policy == WorkflowIDReusePolicy.TERMINATE_IF_RUNNING
                ):
                    await self._dbos_client.cancel_workflow_async(
                        current_status.workflow_id
                    )
                else:
                    raise exceptions.WorkflowAlreadyStartedError(
                        id, type_name, run_id=current_status.workflow_id
                    )
            # Reuse policies (vs a closed run).
            if id_reuse_policy == WorkflowIDReusePolicy.REJECT_DUPLICATE:
                raise exceptions.WorkflowAlreadyStartedError(
                    id, type_name, run_id=current_status.workflow_id
                )
            if (
                id_reuse_policy == WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY
                and _status.to_execution_status(current_status.status)
                == WorkflowExecutionStatus.COMPLETED
            ):
                raise exceptions.WorkflowAlreadyStartedError(
                    id, type_name, run_id=current_status.workflow_id
                )
            run_index = current_index + 1

        dbos_id = ids.run_dbos_id(id, run_index)
        options: EnqueueOptions = {
            "workflow_name": f"wf:{type_name}",
            "queue_name": task_queue,
            "workflow_id": dbos_id,
        }
        if run_timeout is not None:
            options["workflow_timeout"] = run_timeout.total_seconds()
        if start_delay is not None:
            options["delay_seconds"] = start_delay.total_seconds()
        await self._dbos_client.enqueue_async(options, workflow_args)

        if start_signal is not None:
            await self._dbos_client.send_async(
                dbos_id,
                inbox.signal_envelope(start_signal, list(start_signal_args)),
                inbox.INBOX_TOPIC,
            )
        # Like temporalio, the returned handle is NOT run-bound: signals,
        # queries, and updates resolve the chain's *current* run at call
        # time, so they keep routing correctly across continue-as-new (a
        # run-bound handle from get_workflow_handle(run_id=...) pins the
        # run, also matching temporalio).
        return WorkflowHandle(
            self,
            id,
            result_run_id=dbos_id,
            # The run THIS start created (a reuse start "begins" at its own
            # run), matching temporalio's response semantics.
            first_execution_run_id=dbos_id,
        )

    async def execute_workflow(
        self,
        workflow: Any,
        arg: Any = _arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: Optional[type] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: WorkflowIDReusePolicy = WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: WorkflowIDConflictPolicy = WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[Any] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: Optional[Any] = None,
        **unsupported: Any,
    ) -> Any:
        """Start a workflow and wait for completion. See ``start_workflow``."""
        handle = await self.start_workflow(
            workflow,
            arg,
            args=args,
            id=id,
            task_queue=task_queue,
            result_type=result_type,
            execution_timeout=execution_timeout,
            run_timeout=run_timeout,
            task_timeout=task_timeout,
            id_reuse_policy=id_reuse_policy,
            id_conflict_policy=id_conflict_policy,
            retry_policy=retry_policy,
            cron_schedule=cron_schedule,
            memo=memo,
            search_attributes=search_attributes,
            static_summary=static_summary,
            static_details=static_details,
            start_delay=start_delay,
            start_signal=start_signal,
            start_signal_args=start_signal_args,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
            request_eager_start=request_eager_start,
            priority=priority,
            **unsupported,
        )
        return await handle.result()

    def get_workflow_handle(
        self,
        workflow_id: str,
        *,
        run_id: Optional[str] = None,
        first_execution_run_id: Optional[str] = None,
        result_type: Optional[type] = None,
    ) -> "WorkflowHandle":
        """Get a workflow handle. With no ``run_id``, operations target the
        latest run of the id at call time.
        """
        return WorkflowHandle(
            self,
            workflow_id,
            run_id=run_id,
            first_execution_run_id=first_execution_run_id,
        )

    def get_workflow_handle_for(
        self,
        workflow: Any,
        workflow_id: str,
        *,
        run_id: Optional[str] = None,
        first_execution_run_id: Optional[str] = None,
    ) -> "WorkflowHandle":
        """Typed variant of :py:meth:`get_workflow_handle`."""
        return self.get_workflow_handle(
            workflow_id, run_id=run_id, first_execution_run_id=first_execution_run_id
        )

    async def start_update_with_start_workflow(
        self,
        update: Any,
        arg: Any = _arg_unset,
        *,
        start_workflow_operation: WithStartWorkflowOperation,
        wait_for_stage: WorkflowUpdateStage,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        result_type: Optional[type] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowUpdateHandle:
        """Start a workflow (per the operation's id_conflict_policy,
        typically USE_EXISTING) and send it an update, waiting for
        ``wait_for_stage``. Not atomic: the start commits before the update
        is sent (DEVIATIONS.md D7 family); the operation's workflow handle
        is available even if the update fails.
        """
        op = start_workflow_operation
        if op._used:
            raise RuntimeError("WithStartWorkflowOperation cannot be reused")
        op._used = True
        start_args = op._start_kwargs["args"]
        start_kwargs = {k: v for k, v in op._start_kwargs.items() if k != "args"}
        op._handle = await self.start_workflow(
            op._workflow, args=start_args, **start_kwargs
        )
        return await op._handle.start_update(
            update,
            arg,
            wait_for_stage=wait_for_stage,
            args=args,
            id=id,
            result_type=result_type,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )

    async def execute_update_with_start_workflow(
        self,
        update: Any,
        arg: Any = _arg_unset,
        *,
        start_workflow_operation: WithStartWorkflowOperation,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        result_type: Optional[type] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any:
        """Start a workflow (if needed) and execute an update on it,
        returning the update result. See
        :py:meth:`start_update_with_start_workflow`.
        """
        handle = await self.start_update_with_start_workflow(
            update,
            arg,
            start_workflow_operation=start_workflow_operation,
            wait_for_stage=WorkflowUpdateStage.COMPLETED,
            args=args,
            id=id,
            result_type=result_type,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )
        return await handle.result(rpc_timeout=rpc_timeout)

    def get_async_activity_handle(
        self,
        *,
        workflow_id: Optional[str] = None,
        run_id: Optional[str] = None,
        activity_id: Optional[str] = None,
        task_token: Optional[bytes] = None,
    ) -> AsyncActivityHandle:
        """Get a handle for completing an activity asynchronously, by task
        token (``activity.info().task_token``) or by workflow_id +
        activity_id (run_id optional: the chain's current run is resolved).
        """
        if task_token is not None:
            return AsyncActivityHandle(self, task_token)
        return AsyncActivityHandle(self, (workflow_id, run_id, activity_id))

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _chain_lookup(self, dbos_ids: Sequence[str]) -> Dict[str, Any]:
        """Batched exact-id status lookup (primary-key reads; never a
        prefix scan — unindexed in DBOS and unsafe on the critical path)."""
        statuses = await self._dbos_client.list_workflows_async(
            workflow_ids=list(dbos_ids)
        )
        return {status.workflow_id: status for status in statuses}

    async def _current_run(
        self, workflow_id: str
    ) -> Optional[tuple[int, WorkflowStatus]]:
        """The highest-index run of a Temporal workflow id, if any."""
        return await ids.resolve_latest_run(workflow_id, self._chain_lookup)

    async def _resolve_dbos_id(self, workflow_id: str, run_id: Optional[str]) -> str:
        if run_id is not None:
            return run_id
        current = await self._current_run(workflow_id)
        if current is None:
            raise RuntimeError(f"Workflow not found: {workflow_id!r}")
        return current[1].workflow_id

    async def _newer_chain_runs(self, workflow_id: str, after_index: int) -> List[str]:
        """DBOS ids of chain runs newer than ``after_index``, newest first.
        Run ids between a known index and the resolved latest exist by
        construction (chains are dense), so no further lookups are needed.
        """
        resolved = await self._current_run(workflow_id)
        if resolved is None:
            return []
        latest = resolved[0]
        return [
            ids.run_dbos_id(workflow_id, index)
            for index in range(latest, after_index, -1)
        ]

    async def _await_reply_event(
        self, workflow_id: str, target: str, key: str, timeout_seconds: float
    ) -> Optional[Any]:
        """Wait for a reply event (update acceptance/result, query reply),
        accounting for continue-as-new: a message still unconsumed when
        ``target`` continued as new was forwarded to a later run, which
        wrote the reply under *its own* id. Wait on the original target in
        slices; on each miss, sweep newer chain runs (the reply keys are
        globally unique, so finding one anywhere on the chain is
        unambiguous).
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        target_index = ids.run_index_of(workflow_id, target)
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return None
            value = await self._dbos_client.get_event_async(
                target, key, min(REPLY_SWEEP_INTERVAL_SECONDS, remaining)
            )
            if value is not None:
                return value
            if target_index is None:
                continue  # not a chain member; nothing to sweep
            for run_id in await self._newer_chain_runs(workflow_id, target_index):
                value = await self._dbos_client.get_event_async(run_id, key, 0)
                if value is not None:
                    return value

    async def _apply_parent_close_policies(
        self, parent_dbos_id: str, visited: "set[str]"
    ) -> None:
        """Apply the ParentClosePolicy recorded in the parent's children
        event (see inbox.CHILDREN_EVENT_KEY) after a termination."""
        if parent_dbos_id in visited:
            return
        visited.add(parent_dbos_id)
        children = await self._dbos_client.get_event_async(
            parent_dbos_id, inbox.CHILDREN_EVENT_KEY, 0
        )
        if not children:
            return
        for child in children:
            child_id, policy = child["id"], child.get("policy", 1)
            if policy == 2:  # ABANDON
                continue
            # The policy applies to the child's *chain* — a child that
            # continued as new lives at a later run.
            resolved = await self._current_run(child_id)
            if resolved is None or not _status.is_open(resolved[1].status):
                continue
            current_id = resolved[1].workflow_id
            if policy == 3:  # REQUEST_CANCEL
                await self._dbos_client.send_async(
                    current_id, inbox.cancel_envelope(), inbox.INBOX_TOPIC
                )
            else:  # TERMINATE / UNSPECIFIED
                await self._dbos_client.cancel_workflow_async(current_id)
                await self._apply_parent_close_policies(current_id, visited)

    async def _status_of(self, dbos_id: str) -> WorkflowStatus:
        statuses = await self._dbos_client.list_workflows_async(workflow_ids=[dbos_id])
        if not statuses:
            raise RuntimeError(f"Workflow run not found: {dbos_id!r}")
        return statuses[0]


class WorkflowHandle:
    """Handle for interacting with a workflow.

    Created via :py:meth:`Client.start_workflow` or
    :py:meth:`Client.get_workflow_handle`.
    """

    def __init__(
        self,
        client: Client,
        id: str,
        *,
        run_id: Optional[str] = None,
        result_run_id: Optional[str] = None,
        first_execution_run_id: Optional[str] = None,
    ) -> None:
        self._client = client
        self._id = id
        self._run_id = run_id
        self._result_run_id = result_run_id
        self._first_execution_run_id = first_execution_run_id

    @property
    def id(self) -> str:
        """ID of the workflow."""
        return self._id

    @property
    def run_id(self) -> Optional[str]:
        """Run ID used for signals/queries/updates if bound (signals on an
        unbound handle target the chain's current run, as in temporalio)."""
        return self._run_id

    @property
    def result_run_id(self) -> Optional[str]:
        """Run ID that :py:meth:`result` anchors on (the run a
        ``start_workflow`` created), following the chain from there."""
        return self._result_run_id

    @property
    def first_execution_run_id(self) -> Optional[str]:
        """First run ID of the workflow id's chain, if known."""
        return self._first_execution_run_id

    async def _target(self) -> str:
        return await self._client._resolve_dbos_id(self._id, self._run_id)

    async def result(
        self,
        *,
        follow_runs: bool = True,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any:
        """Wait for and return the result; raises
        :py:class:`WorkflowFailureError` on workflow failure with the exact
        cause reconstructed. (rpc_timeout bounds a single RPC in temporalio,
        not the total wait, so it is accepted and ignored here.)
        """
        _ignore_rpc_options("result", rpc_metadata, rpc_timeout)
        # Anchor on the run this handle's start created when known (so a
        # later id-reuse can't redirect the wait), else the current run.
        dbos_id = self._result_run_id or await self._target()
        runtime_client = self._client._dbos_client
        while True:
            handle: Any = await runtime_client.retrieve_workflow_async(dbos_id)
            try:
                return await handle.get_result()
            except SerializedContinueAsNew as marker:
                new_run_id: str = marker.envelope["new_run_id"]
                if not follow_runs:
                    raise WorkflowContinuedAsNewError(new_run_id) from None
                dbos_id = new_run_id
                continue
            except SerializedWorkflowFailure as failure:
                # NOTE: `raise ... from X` overwrites __cause__, which the
                # constructor just set — so the `from` target must be the
                # cause itself.
                cause = deserialize_failure(failure.envelope)
                raise WorkflowFailureError(cause=cause) from cause
            except DBOSAwaitedWorkflowCancelledError:
                # Native DBOS cancel == terminate in our scheme (§6.5).
                terminated = exceptions.TerminatedError("Workflow terminated")
                raise WorkflowFailureError(cause=terminated) from terminated
            except Exception as err:
                # FAIL_FAST mode or infrastructure errors: surface with a
                # converted cause rather than a raw pickled exception.
                converted = exceptions.ApplicationError(
                    str(err), type=type(err).__name__
                )
                converted.__cause__ = err
                raise WorkflowFailureError(cause=converted) from converted

    async def signal(
        self,
        signal: Any,
        arg: Any = _arg_unset,
        *,
        args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Send a signal to the workflow."""
        _ignore_rpc_options("signal", rpc_metadata, None)
        await self._client._dbos_client.send_async(
            await self._target(),
            inbox.signal_envelope(_signal_name(signal), _resolve_args(arg, args)),
            inbox.INBOX_TOPIC,
        )

    async def query(
        self,
        query: Any,
        arg: Any = _arg_unset,
        *,
        args: Sequence[Any] = [],
        result_type: Optional[type] = None,
        reject_condition: Optional[QueryRejectCondition] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any:
        """Query the workflow (v1: requires a RUNNING workflow). Raises
        :py:class:`WorkflowQueryRejectedError` if the workflow's status
        matches ``reject_condition`` (or the client default).
        """
        _ignore_rpc_options("query", rpc_metadata, None)
        condition = reject_condition or self._client._default_query_reject_condition
        if condition is not None and condition != QueryRejectCondition.NONE:
            # Client-side check (no server arbiter — DEVIATIONS D7 family):
            # the status read and the query send are not atomic.
            status = (await self.describe()).status
            rejected = (
                status != WorkflowExecutionStatus.RUNNING
                if condition == QueryRejectCondition.NOT_OPEN
                else status != WorkflowExecutionStatus.COMPLETED
            )
            if rejected:
                raise WorkflowQueryRejectedError(status)
        request_id = str(uuid_mod.uuid4())
        client = self._client._dbos_client
        target = await self._target()
        timeout = rpc_timeout.total_seconds() if rpc_timeout else 60.0
        await client.send_async(
            target,
            inbox.query_envelope(
                _query_name(query), _resolve_args(arg, args), request_id
            ),
            inbox.INBOX_TOPIC,
        )
        reply = await self._client._await_reply_event(
            self._id, target, inbox.query_result_key(request_id), timeout
        )
        if reply is None:
            raise WorkflowQueryFailedError(
                f"query did not complete within {timeout}s (v1 queries "
                "require a RUNNING workflow; see README deviations)"
            )
        if reply["status"] == "completed":
            return reply["result"]
        raise WorkflowQueryFailedError(str(deserialize_failure(reply["failure"])))

    async def start_update(
        self,
        update: Any,
        arg: Any = _arg_unset,
        *,
        wait_for_stage: WorkflowUpdateStage,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        result_type: Optional[type] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowUpdateHandle:
        """Send an update and wait until it reaches ``wait_for_stage``
        (ACCEPTED: past its validator; COMPLETED: handler finished). Raises
        :py:class:`WorkflowUpdateFailedError` if the update is rejected.
        """
        _ignore_rpc_options("start_update", rpc_metadata, None)
        if wait_for_stage not in (
            WorkflowUpdateStage.ACCEPTED,
            WorkflowUpdateStage.COMPLETED,
        ):
            raise ValueError("Admitted wait stage not supported")
        update_id = id or str(uuid_mod.uuid4())
        client = self._client._dbos_client
        target = await self._target()
        timeout = rpc_timeout.total_seconds() if rpc_timeout else 60.0
        await client.send_async(
            target,
            inbox.update_envelope(
                _update_name(update), _resolve_args(arg, args), update_id
            ),
            inbox.INBOX_TOPIC,
            idempotency_key=update_id,
        )
        handle = WorkflowUpdateHandle(
            self._client, update_id, self._id, workflow_run_id=target
        )
        if wait_for_stage == WorkflowUpdateStage.ACCEPTED:
            acceptance = await self._client._await_reply_event(
                self._id, target, inbox.update_acceptance_key(update_id), timeout
            )
            if acceptance is None:
                raise TimeoutError(f"update was not accepted within {timeout}s")
            if acceptance["status"] != "accepted":
                raise WorkflowUpdateFailedError(
                    deserialize_failure(acceptance["failure"])
                )
        else:  # COMPLETED
            outcome = await self._client._await_reply_event(
                self._id, target, inbox.update_result_key(update_id), timeout
            )
            if outcome is None:
                raise TimeoutError(f"update did not complete within {timeout}s")
            if outcome["status"] == "rejected":
                # Rejection is a failure of the *start* (never accepted).
                raise WorkflowUpdateFailedError(deserialize_failure(outcome["failure"]))
            handle._known_outcome = outcome
        return handle

    async def execute_update(
        self,
        update: Any,
        arg: Any = _arg_unset,
        *,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        result_type: Optional[type] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any:
        """Send an update and wait for its result; raises
        :py:class:`WorkflowUpdateFailedError` on rejection or failure.
        """
        handle = await self.start_update(
            update,
            arg,
            wait_for_stage=WorkflowUpdateStage.COMPLETED,
            args=args,
            id=id,
            result_type=result_type,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )
        return await handle.result(rpc_timeout=rpc_timeout)

    def get_update_handle(
        self,
        id: str,
        *,
        workflow_run_id: Optional[str] = None,
        result_type: Optional[type] = None,
    ) -> WorkflowUpdateHandle:
        """Get a handle for an already-sent update — e.g. to re-attach and
        collect its result after a client restart. Requires the update to
        have been started with a known ``id``.
        """
        return WorkflowUpdateHandle(
            self._client,
            id,
            self._id,
            workflow_run_id=workflow_run_id or self._run_id,
            result_type=result_type,
        )

    def get_update_handle_for(
        self,
        update: Any,
        id: str,
        *,
        workflow_run_id: Optional[str] = None,
    ) -> WorkflowUpdateHandle:
        """Get a typed handle for an already-sent update (see
        :py:meth:`get_update_handle`)."""
        return self.get_update_handle(id, workflow_run_id=workflow_run_id)

    async def describe(
        self,
        *,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowExecutionDescription:
        """Get the current description of this workflow's latest (or bound)
        run."""
        _ignore_rpc_options("describe", rpc_metadata, rpc_timeout)
        dbos_id = await self._target()
        status = await self._client._status_of(dbos_id)
        workflow_type = status.name or ""
        if workflow_type.startswith("wf:"):
            workflow_type = workflow_type[3:]
        return WorkflowExecutionDescription(
            id=self._id,
            run_id=status.workflow_id,
            workflow_type=workflow_type,
            task_queue=status.queue_name,
            status=_status.to_execution_status(status.status, error=status.error),
            start_time=_to_datetime(status.created_at),
            close_time=_to_datetime(status.completed_at),
            # A same-chain DBOS parent link is a continuation
            # (continue-as-new), not a parent (Info.continued_run_id
            # territory); only cross-chain links are real parents.
            parent_id=(
                status.parent_workflow_id
                if status.parent_workflow_id is not None
                and ids.parse_run(status.parent_workflow_id)[0]
                != ids.parse_run(status.workflow_id)[0]
                else None
            ),
        )

    async def cancel(
        self,
        *,
        reason: str = "",
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Request cooperative cancellation (§6.5): the workflow's primary
        coroutine gets CancelledError at its next event boundary; cleanup
        code runs and may still execute activities. The workflow may also
        swallow the cancel and complete normally.
        """
        _ignore_rpc_options("cancel", rpc_metadata, rpc_timeout)
        await self._client._dbos_client.send_async(
            await self._target(), inbox.cancel_envelope(reason), inbox.INBOX_TOPIC
        )

    async def terminate(
        self,
        *args: Any,
        reason: Optional[str] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Forcefully terminate (§6.5): native DBOS cancellation. No
        workflow code runs; status becomes TERMINATED. ``reason``/details
        are accepted but not stored (DBOS cancellation has no reason field).
        """
        _ignore_rpc_options("terminate", rpc_metadata, rpc_timeout)
        if args or reason:
            logger.debug("terminate: reason/details are not stored")
        target = await self._target()
        # Terminate must land on a LIVE run. Two hazards (Temporal's server
        # handles both atomically): a DBOS cancel on a closed run would
        # CLOBBER its recorded status (eating a continue-as-new marker), so
        # closed targets raise instead; and a workflow hopping via
        # continue-as-new mid-request must not escape, so after each cancel
        # we follow any CAN-created successor (identified by its same-chain
        # parent link — a reuse-created successor is a new logical
        # execution and is left alone) and terminate it too.
        bound = self._run_id is not None
        cancelled_any = False
        for _ in range(64):
            status = await self._client._status_of(target)
            mapped = _status.to_execution_status(status.status, error=status.error)
            if mapped == WorkflowExecutionStatus.CONTINUED_AS_NEW and (
                not bound or cancelled_any
            ):
                # Unbound terminates address the workflow: follow the hop.
                # (A run-bound terminate on a closed run raises below, as in
                # Temporal.)
                base, index = ids.parse_run(target)
                target = ids.run_dbos_id(base, index + 1)
                continue
            if mapped != WorkflowExecutionStatus.RUNNING:
                raise RuntimeError(
                    f"Workflow run already closed: {target!r} ({mapped.name})"
                )
            await self._client._dbos_client.cancel_workflow_async(target)
            # Termination runs no workflow code, so the close sweep never
            # fires: apply the durably-recorded ParentClosePolicy of each
            # child (recursively) from here.
            await self._client._apply_parent_close_policies(target, set())
            cancelled_any = True
            base, index = ids.parse_run(target)
            next_id = ids.run_dbos_id(base, index + 1)
            successors = await self._client._dbos_client.list_workflows_async(
                workflow_ids=[next_id]
            )
            if not successors or successors[0].parent_workflow_id != target:
                return
            target = next_id
        raise RuntimeError(
            "terminate did not converge: the workflow kept continuing-as-new"
        )

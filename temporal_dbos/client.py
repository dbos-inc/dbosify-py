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
import inspect
import json
import logging
import uuid as uuid_mod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import IntEnum
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    overload,
)

from dbos import DBOSClient, EnqueueOptions, WorkflowStatus
from dbos._error import DBOSAwaitedWorkflowCancelledError

from . import _schedule, exceptions
from ._internal import attributes as _attributes
from ._internal import conversion, ids, inbox
from ._internal import registry as _registry
from ._internal import replay as _replay
from ._internal import schedules as _schedules
from ._internal import status as _status
from ._internal import visibility as _visibility
from ._internal.client_interceptor import (
    BackfillScheduleInput,
    CancelWorkflowInput,
    CompleteAsyncActivityInput,
    CreateScheduleInput,
    DeleteScheduleInput,
    DescribeScheduleInput,
    DescribeWorkflowInput,
    FailAsyncActivityInput,
    HeartbeatAsyncActivityInput,
    Interceptor,
    ListSchedulesInput,
    OutboundInterceptor,
    PauseScheduleInput,
    QueryWorkflowInput,
    ReportCancellationAsyncActivityInput,
    SignalWorkflowInput,
    StartWorkflowInput,
    StartWorkflowUpdateInput,
    TerminateWorkflowInput,
    TriggerScheduleInput,
    UnpauseScheduleInput,
    UpdateScheduleInput,
)
from ._internal.namespaces import (
    DEFAULT_NAMESPACE,
    namespace_from_schema,
    namespace_schema,
)
from ._internal.payloads import (
    RunMeta,
    SerializedContinueAsNew,
    SerializedWorkflowFailure,
    deserialize_failure,
    serialize_failure,
    serialize_retry_policy,
    wrap_input,
)
from ._internal.serializer import TEMPORAL_SERIALIZER
from ._internal.status import WorkflowExecutionStatus

# Schedule types (DESIGN §6.7) live in _schedule.py and are re-exported here to
# mirror temporalio.client's namespace.
from ._schedule import (  # noqa: E402
    Schedule,
    ScheduleAction,
    ScheduleActionExecution,
    ScheduleActionExecutionStartWorkflow,
    ScheduleActionResult,
    ScheduleActionStartWorkflow,
    ScheduleAsyncIterator,
    ScheduleBackfill,
    ScheduleCalendarSpec,
    ScheduleDescription,
    ScheduleHandle,
    ScheduleInfo,
    ScheduleIntervalSpec,
    ScheduleListAction,
    ScheduleListActionStartWorkflow,
    ScheduleListDescription,
    ScheduleListInfo,
    ScheduleListSchedule,
    ScheduleListState,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
    ScheduleUpdateInput,
)
from .common import (
    QueryRejectCondition,
    RetryPolicy,
    SearchAttributes,
    TypedSearchAttributes,
    WorkflowIDConflictPolicy,
    WorkflowIDReusePolicy,
    _warn_on_deprecated_search_attributes,
)
from .converter import DataConverter
from .workflow import _UpdateMethod

# How often reply waits (update acceptance/result, query replies) re-check
# newer runs of the chain: a message still unconsumed when its target run
# continues-as-new is forwarded to (and answered under) a later run's id.
REPLY_SWEEP_INTERVAL_SECONDS = 1.0

__all__ = [
    "AsyncActivityCancelledError",
    "AsyncActivityHandle",
    "BackfillScheduleInput",
    "CancelWorkflowInput",
    "Client",
    "CompleteAsyncActivityInput",
    "CreateScheduleInput",
    "DeleteScheduleInput",
    "DescribeScheduleInput",
    "DescribeWorkflowInput",
    "FailAsyncActivityInput",
    "HeartbeatAsyncActivityInput",
    "Interceptor",
    "ListSchedulesInput",
    "OutboundInterceptor",
    "PauseScheduleInput",
    "QueryWorkflowInput",
    "ReportCancellationAsyncActivityInput",
    "SignalWorkflowInput",
    "StartWorkflowInput",
    "StartWorkflowUpdateInput",
    "TerminateWorkflowInput",
    "TriggerScheduleInput",
    "UnpauseScheduleInput",
    "UpdateScheduleInput",
    "Schedule",
    "ScheduleAction",
    "ScheduleActionExecution",
    "ScheduleActionExecutionStartWorkflow",
    "ScheduleActionResult",
    "ScheduleActionStartWorkflow",
    "ScheduleAsyncIterator",
    "ScheduleBackfill",
    "ScheduleCalendarSpec",
    "ScheduleDescription",
    "ScheduleHandle",
    "ScheduleInfo",
    "ScheduleIntervalSpec",
    "ScheduleListAction",
    "ScheduleListActionStartWorkflow",
    "ScheduleListDescription",
    "ScheduleListInfo",
    "ScheduleListSchedule",
    "ScheduleListState",
    "ScheduleOverlapPolicy",
    "SchedulePolicy",
    "ScheduleRange",
    "ScheduleSpec",
    "ScheduleState",
    "ScheduleUpdate",
    "ScheduleUpdateInput",
    "WithStartWorkflowOperation",
    "WorkflowContinuedAsNewError",
    "WorkflowQueryRejectedError",
    "WorkflowHandle",
    "WorkflowExecution",
    "WorkflowExecutionAsyncIterator",
    "WorkflowExecutionCount",
    "WorkflowExecutionCountAggregationGroup",
    "WorkflowExecutionDescription",
    "WorkflowExecutionStatus",
    "WorkflowFailureError",
    "WorkflowHistory",
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
        self._known_outcome = known_outcome
        self._result_type = result_type

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
            return await conversion.decode_value(outcome["result"], self._result_type)
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

    def __init__(
        self,
        client: "Client",
        id_or_token: Any,
        data_converter_override: Optional[DataConverter] = None,
    ) -> None:
        self._client = client
        # The original addressing argument, re-used to rebuild this handle at
        # the root of the outbound chain (DESIGN §6.8).
        self._id_or_token = id_or_token
        # Per-handle converter override for complete/fail/heartbeat encoding. It
        # rides on each *Input so it survives the chain-root handle rebuild
        # (which only carries id_or_token).
        self._converter = data_converter_override
        self._workflow_id: Optional[str] = None
        self._run_id: Optional[str] = None
        # On the queued path (§6.1.2) the completion goes to the activity
        # workflow's recv topic, not the parent run's inbox.
        self._queued_wf: Optional[str] = None
        if isinstance(id_or_token, bytes):
            token = json.loads(id_or_token.decode())
            self._run_id = token["run"]
            self._activity_id: str = token["aid"]
            self._queued_wf = token.get("qwf")
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
        if self._queued_wf is not None:
            return self._queued_wf
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
        # Queued activities park on a dedicated completion topic; local ones
        # consume from the shared inbox.
        topic = (
            inbox.ASYNC_COMPLETE_TOPIC
            if self._queued_wf is not None
            else inbox.INBOX_TOPIC
        )
        await self._client._dbos_client.send_async(target, envelope, topic)

    async def complete(
        self,
        result: Optional[Any] = _arg_unset,
        *,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Complete the activity with a result."""
        _ignore_rpc_options("async activity complete", rpc_metadata, rpc_timeout)
        await self._client._impl.complete_async_activity(
            CompleteAsyncActivityInput(
                id_or_token=self._id_or_token,
                result=None if result is _arg_unset else result,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                data_converter_override=self._converter,
            )
        )

    async def _complete_impl(self, input: CompleteAsyncActivityInput) -> None:
        await self._send(
            inbox.activity_result_envelope(
                self._activity_id,
                result=await conversion.encode_value(
                    input.result, input.data_converter_override
                ),
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
        await self._client._impl.fail_async_activity(
            FailAsyncActivityInput(
                id_or_token=self._id_or_token,
                error=error,
                last_heartbeat_details=last_heartbeat_details,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                data_converter_override=self._converter,
            )
        )

    async def _fail_impl(self, input: FailAsyncActivityInput) -> None:
        await self._send(
            inbox.activity_result_envelope(
                self._activity_id,
                failure=serialize_failure(input.error, input.data_converter_override),
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
        await self._client._impl.heartbeat_async_activity(
            HeartbeatAsyncActivityInput(
                id_or_token=self._id_or_token,
                details=details,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                data_converter_override=self._converter,
            )
        )

    async def _heartbeat_impl(self, input: HeartbeatAsyncActivityInput) -> None:
        await self._send(
            inbox.activity_heartbeat_envelope(
                self._activity_id,
                await conversion.encode_values(
                    list(input.details), input.data_converter_override
                ),
            )
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
        await self._client._impl.report_cancellation_async_activity(
            ReportCancellationAsyncActivityInput(
                id_or_token=self._id_or_token,
                details=details,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def _report_cancellation_impl(
        self, input: ReportCancellationAsyncActivityInput
    ) -> None:
        topic = (
            inbox.ASYNC_COMPLETE_TOPIC
            if self._queued_wf is not None
            else inbox.INBOX_TOPIC
        )
        await self._client._dbos_client.send_async(
            await self._target(),
            inbox.activity_result_envelope(self._activity_id, cancelled=True),
            topic,
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
            if self._used:
                # Used, but the workflow start itself raised (e.g. a FAIL /
                # REJECT_DUPLICATE conflict), so no run was started/attached.
                raise RuntimeError(
                    "WithStartWorkflowOperation was used but the workflow "
                    "start did not complete; no handle is available"
                )
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
    # When this run started or should start. We have a single creation
    # timestamp per run, so this equals :py:attr:`start_time`.
    execution_time: Optional[datetime] = None
    id: str = ""
    namespace: str = "default"
    parent_id: Optional[str] = None
    # Run ID of the parent (its DBOS run id); set only for cross-chain children.
    parent_run_id: Optional[str] = None
    run_id: str = ""
    search_attributes: SearchAttributes = field(default_factory=dict)
    """Search attributes for the workflow.

    .. deprecated::
        Use :py:attr:`typed_search_attributes` instead.
    """
    start_time: Optional[datetime] = None
    status: Optional[WorkflowExecutionStatus] = None
    task_queue: Optional[str] = None
    typed_search_attributes: TypedSearchAttributes = TypedSearchAttributes.empty
    """Search attributes for the workflow."""
    workflow_type: str = ""
    # The stored (converter-encoded) memo, decoded lazily by ``memo()`` /
    # ``memo_value()`` — matches temporalio, where memo decode is async. Not a
    # constructor parameter (temporalio reads memo from ``raw_info`` instead);
    # describe() sets it via object.__setattr__ on the frozen instance.
    _encoded_memo: Mapping[str, Any] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )

    async def memo(self) -> Mapping[str, Any]:
        """Workflow's memo values, converted without type hints."""
        return await _attributes.decode_memo(self._encoded_memo)

    @overload
    async def memo_value(
        self, key: str, *, type_hint: Optional[type] = None
    ) -> Any: ...

    @overload
    async def memo_value(
        self, key: str, default: Any, *, type_hint: Optional[type] = None
    ) -> Any: ...

    async def memo_value(
        self,
        key: str,
        default: Any = _arg_unset,
        *,
        type_hint: Optional[type] = None,
    ) -> Any:
        """Memo value for the given key, optionally rebuilt to ``type_hint``.

        Raises ``KeyError`` if the key is absent and no ``default`` is given.
        """
        encoded = self._encoded_memo.get(key, _arg_unset)
        if encoded is _arg_unset:
            if default is _arg_unset:
                raise KeyError(f"Memo does not have a value for key {key}")
            return default
        return await conversion.decode_value(encoded, type_hint)


@dataclass(frozen=True)
class WorkflowExecutionDescription(WorkflowExecution):
    """Description for a single workflow execution run."""


@dataclass(frozen=True)
class WorkflowExecutionCountAggregationGroup:
    """Aggregation group if the count query had a group-by clause.

    We don't parse group-by (DEVIATION), so this is never populated; it exists
    for shape parity with temporalio.
    """

    count: int
    group_values: Sequence[Any]


@dataclass(frozen=True)
class WorkflowExecutionCount:
    """Representation of a count from a ``count_workflows`` call."""

    count: int
    groups: Sequence[WorkflowExecutionCountAggregationGroup]


@dataclass(frozen=True)
class WorkflowHistory:
    """A workflow run's recorded execution, as the :class:`Replayer` consumes it.

    Mirrors ``temporalio.client.WorkflowHistory`` in name and role, but its
    contents are DBOS-native: rather than a Temporal event log, it carries the
    run's DBOS-recorded step checkpoints (``recorded_steps``) plus the inputs
    and attributes needed to re-execute it. v1 is **DB-bound** — it references a
    live DBOS run (``run_id``) that the replay engine forks; there is no offline
    JSON portability (a future ``from_json``/``to_json`` would slot in here).
    Built by :py:meth:`WorkflowHandle.fetch_history`.
    """

    workflow_id: str
    """Temporal workflow id (the run-chain base)."""
    run_id: str
    """DBOS run id whose checkpoints back this history."""
    workflow_type: str
    """Workflow type name (the ``wf:`` dispatcher prefix stripped)."""
    input: Any = None
    """Recorded dispatcher payload, kept opaque — the fork re-feeds it verbatim."""
    recorded_steps: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    """The run's ``StepInfo`` checkpoints (``DBOS.list_workflow_steps`` order)."""
    status: Optional[WorkflowExecutionStatus] = None
    attributes: Mapping[str, Any] = field(default_factory=dict)
    """Raw DBOS attributes column (memo + search attributes), unparsed."""
    app_version: Optional[str] = None

    @property
    def replay_horizon(self) -> int:
        """Highest recorded ``function_id`` — the checkpoint horizon a replay
        may reach (0 when no steps were recorded)."""
        return max((s["function_id"] for s in self.recorded_steps), default=0)

    @property
    def step_count(self) -> int:
        """Number of recorded steps."""
        return len(self.recorded_steps)


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


def _result_type_for(workflow: Any, result_type: Optional[type]) -> Optional[type]:
    """The type to rebuild a workflow's result into: an explicit ``result_type``
    wins; otherwise infer the run method's return annotation — from the local
    registry, or (thin client, no worker) from a passed run-method reference."""
    if result_type is not None:
        return result_type
    try:
        return _registry.lookup_workflow(_workflow_type_name(workflow)).ret_type
    except (KeyError, TypeError):
        pass
    if inspect.isfunction(workflow):
        _, ret = conversion.type_hints_from_func(workflow)
        return ret
    return None


def _signal_name(signal: Any) -> str:
    if isinstance(signal, str):
        return signal
    # A dynamic handler's marker value is None (vs absent for a non-handler):
    # it has no name, so it can only be addressed by string.
    if not hasattr(signal, _registry.SIGNAL_ATTR):
        raise TypeError(f"{signal!r} is not a @workflow.signal method or name")
    name = getattr(signal, _registry.SIGNAL_ATTR)
    if name is None:
        raise TypeError(
            "Cannot reference a dynamic signal handler by method; pass the "
            "signal name as a string"
        )
    return str(name)


def _query_name(query: Any) -> str:
    if isinstance(query, str):
        return query
    if not hasattr(query, _registry.QUERY_ATTR):
        raise TypeError(f"{query!r} is not a @workflow.query method or name")
    name = getattr(query, _registry.QUERY_ATTR)
    if name is None:
        raise TypeError(
            "Cannot reference a dynamic query handler by method; pass the "
            "query name as a string"
        )
    return str(name)


def _update_name(update: Any) -> str:
    if isinstance(update, str):
        return update
    if isinstance(update, _UpdateMethod):
        if update.name is None:
            raise TypeError(
                "Cannot reference a dynamic update handler by method; pass the "
                "update name as a string"
            )
        return update.name
    raise TypeError(f"{update!r} is not a @workflow.update method or name")


def _install_serializer(dbos_client: DBOSClient) -> None:
    """Install the JSON transport serializer on a (user-created) DBOSClient so
    it matches the Worker's (DBOS selects the deserializer by row label and
    rejects a mismatch). Relies on DBOS internals (no public setter)."""
    dbos_client._serializer = TEMPORAL_SERIALIZER
    sys_db = getattr(dbos_client, "_sys_db", None)
    if sys_db is not None:
        sys_db.serializer = TEMPORAL_SERIALIZER


def _ref_ret_type(ref: Any, result_type: Optional[type]) -> Optional[type]:
    """Result type for an update/query reply: an explicit ``result_type``
    wins, else the handler reference's return annotation (a ``_UpdateMethod``
    carries its function in ``.fn``; a query method is the function itself)."""
    if result_type is not None:
        return result_type
    fn = ref.fn if isinstance(ref, _UpdateMethod) else ref
    if inspect.isfunction(fn):
        _, ret = conversion.type_hints_from_func(fn)
        return ret
    return None


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


def _execution_from_status(
    status: WorkflowStatus,
    cls: Type[WorkflowExecution] = WorkflowExecution,
    namespace: str = DEFAULT_NAMESPACE,
) -> WorkflowExecution:
    """Synthesize a :class:`WorkflowExecution` (or a subclass — ``describe()``
    passes :class:`WorkflowExecutionDescription`) from a DBOS ``WorkflowStatus``.

    The DBOS workflow id is the run id (decision §10.3); the Temporal workflow
    id is its run-chain base. The DBOS workflow name is ``wf:{type}``.
    """
    workflow_type = status.name or ""
    if workflow_type.startswith("wf:"):
        workflow_type = workflow_type[3:]
    stored_attrs = status.attributes or {}
    typed_sa = _attributes.decode_search_attributes(
        stored_attrs.get(_attributes.SEARCH_ATTRIBUTES_KEY, {})
    )
    dbos_id = status.workflow_id
    # A same-chain DBOS parent link is a continuation (continue-as-new), not a
    # parent; only cross-chain links are real parents.
    parent_dbos_id = (
        status.parent_workflow_id
        if status.parent_workflow_id is not None
        and ids.parse_run(status.parent_workflow_id)[0] != ids.parse_run(dbos_id)[0]
        else None
    )
    created = _to_datetime(status.created_at)
    execution = cls(
        id=ids.parse_run(dbos_id)[0],
        namespace=namespace,
        run_id=dbos_id,
        workflow_type=workflow_type,
        task_queue=status.queue_name,
        status=_status.to_execution_status(status.status, error=status.error),
        start_time=created,
        execution_time=created,
        close_time=_to_datetime(status.completed_at),
        search_attributes=_attributes.typed_to_untyped(typed_sa),
        typed_search_attributes=typed_sa,
        parent_id=ids.parse_run(parent_dbos_id)[0] if parent_dbos_id else None,
        parent_run_id=parent_dbos_id,
    )
    object.__setattr__(
        execution, "_encoded_memo", stored_attrs.get(_attributes.MEMO_KEY, {})
    )
    return execution


def _ignore_rpc_options(
    where: str, rpc_metadata: Mapping[str, Any], rpc_timeout: Optional[timedelta]
) -> None:
    """RPC transport options have no temporal-dbos equivalent; accept and
    debug-log them (DESIGN convention for tuning parameters)."""
    if rpc_metadata:
        logger.debug("%s: ignoring rpc_metadata", where)
    if rpc_timeout is not None:
        logger.debug("%s: ignoring rpc_timeout", where)


class WorkflowExecutionAsyncIterator:
    """Async iterator over :class:`WorkflowExecution` values, as returned by
    :py:meth:`Client.list_workflows`. Most callers just ``async for`` over it.

    Pagination rides DBOS ``limit``/``offset`` rather than an opaque server
    cursor, so :py:attr:`next_page_token` encodes the next raw offset. The
    *raw* offset (DBOS rows scanned) is tracked separately from the count of
    yielded rows, so post-filtering (the ERROR-family ``ExecutionStatus`` and
    ``WorkflowType !=`` cases DBOS can't express) never misaligns pages.
    """

    def __init__(
        self,
        client: "Client",
        *,
        query: Optional[str],
        page_size: int,
        limit: Optional[int],
        next_page_token: Optional[bytes] = None,
    ) -> None:
        self._client = client
        self._query = query
        self._page_size = page_size
        self._limit = limit
        self._fetch_offset = int(next_page_token) if next_page_token else 0
        self._next_page_token: Optional[bytes] = next_page_token
        self._current_page: Optional[Sequence[WorkflowExecution]] = None
        self._current_page_index = 0
        self._yielded = 0
        # The query is parsed lazily on the first fetch, so (as in temporalio)
        # no work happens until iteration begins and a bad query surfaces then.
        self._parsed = False
        self._dbos_filters: Dict[str, Any] = {}
        self._post_filter: Optional[Callable[[WorkflowStatus], bool]] = None

    def _ensure_parsed(self) -> None:
        if not self._parsed:
            parsed = _visibility.parse_query(self._query)
            self._dbos_filters = parsed.to_dbos_filters()
            self._post_filter = parsed.post_filter()
            self._parsed = True

    @property
    def current_page_index(self) -> int:
        """Index of the entry in the current page returned next."""
        return self._current_page_index

    @property
    def current_page(self) -> Optional[Sequence[WorkflowExecution]]:
        """Current page, if it has been fetched yet."""
        return self._current_page

    @property
    def next_page_token(self) -> Optional[bytes]:
        """Token for the next page request if any."""
        return self._next_page_token

    async def fetch_next_page(self, *, page_size: Optional[int] = None) -> None:
        """Fetch the next page if any."""
        self._ensure_parsed()
        size = page_size or self._page_size
        raw = await self._client._dbos_client.list_workflows_async(
            load_input=False,
            load_output=True,
            sort_desc=True,
            limit=size,
            offset=self._fetch_offset,
            **self._dbos_filters,
        )
        raw_count = len(raw)
        self._fetch_offset += raw_count
        # Only user Temporal workflows (named ``wf:{type}``) are visible: skip
        # DBOS plumbing rows (``__temporal_activity`` / ``__temporal_schedule_fire``).
        survivors = [
            r
            for r in raw
            if (r.name or "").startswith("wf:")
            and (self._post_filter is None or self._post_filter(r))
        ]
        self._current_page = [
            _execution_from_status(r, namespace=self._client._namespace)
            for r in survivors
        ]
        self._current_page_index = 0
        # A full raw page means there may be more rows; a short one is the end.
        # ``size > 0`` guards a 0-page-size caller against an infinite loop.
        self._next_page_token = (
            str(self._fetch_offset).encode() if raw_count == size and size > 0 else None
        )

    def __aiter__(self) -> "WorkflowExecutionAsyncIterator":
        """Return self as the iterator."""
        return self

    async def __anext__(self) -> WorkflowExecution:
        """Next execution, fetching pages (and skipping empty post-filtered
        pages) as needed."""
        if self._limit is not None and self._yielded >= self._limit:
            raise StopAsyncIteration
        while True:
            if self._current_page is None:
                await self.fetch_next_page()
                continue
            if self._current_page_index >= len(self._current_page):
                if self._next_page_token is not None:
                    await self.fetch_next_page()
                    continue
                raise StopAsyncIteration
            ret = self._current_page[self._current_page_index]
            self._current_page_index += 1
            self._yielded += 1
            return ret


class Client:
    """Client for accessing temporal-dbos.

    Use :py:meth:`connect` — ``Client.connect(system_database_url,
    namespace=...)`` builds the underlying ``dbos.DBOSClient`` pointed at the
    namespace's schema (DEVIATIONS D1), so you state the namespace once and
    never touch ``dbos_system_schema``. For full control of the DBOSClient
    (custom engine/pool), build it yourself and use the constructor, where the
    DBOSClient's schema *is* the namespace.
    """

    def __init__(
        self,
        dbos_client: DBOSClient,
        *,
        data_converter: DataConverter = DataConverter.default,
        interceptors: Sequence[Interceptor] = [],
        default_workflow_query_reject_condition: Optional[QueryRejectCondition] = None,
    ) -> None:
        """Low-level constructor over a caller-built ``dbos.DBOSClient``. The
        client's **namespace is its DBOSClient's schema** (DEVIATIONS D1) — the
        single source of truth — so build the DBOSClient with
        ``dbos_system_schema=namespace_schema(<namespace>)``, or just use
        :py:meth:`connect`, which takes a namespace and builds the DBOSClient
        for you. The caller owns this DBOSClient's lifecycle.
        """
        self._dbos_client = dbos_client
        # The DBOSClient's schema *is* the namespace (no separate, redundant
        # namespace argument to keep in sync). Raises if it isn't a temporal
        # namespace schema.
        self._namespace = namespace_from_schema(dbos_client._sys_db.schema)
        # connect() flips this for the DBOSClient it builds, so close() disposes
        # it; a caller-supplied DBOSClient (this path) is the caller's to close.
        self._owns_dbos_client = False
        self._data_converter = data_converter
        self._default_query_reject_condition = default_workflow_query_reject_condition
        # Build the outbound interceptor chain: user interceptors fold (in
        # reverse) over the root that performs the actual DBOS operations
        # (DESIGN §6.8). Public verbs/handles build the matching *Input and
        # call ``self._impl.<verb>(input)``.
        impl: OutboundInterceptor = _ClientOutbound(self)
        for interceptor in reversed(interceptors):
            impl = interceptor.intercept_client(impl)
        self._impl = impl
        # This process encodes start args / decodes results with this
        # converter (a separate worker process decodes args / encodes results
        # with its own — configure both the same, as in Temporal).
        conversion.set_converter(data_converter)
        _install_serializer(dbos_client)

    @property
    def data_converter(self) -> DataConverter:
        """Data converter used by this client."""
        return self._data_converter

    @property
    def namespace(self) -> str:
        """Temporal namespace this client operates in (its DBOS schema)."""
        return self._namespace

    @classmethod
    async def connect(
        cls,
        system_database_url: str,
        *,
        namespace: str = DEFAULT_NAMESPACE,
        data_converter: DataConverter = DataConverter.default,
        interceptors: Sequence[Interceptor] = [],
        default_workflow_query_reject_condition: Optional[QueryRejectCondition] = None,
    ) -> "Client":
        """Connect to ``system_database_url`` in ``namespace`` (DEVIATIONS D1).

        Builds the underlying ``dbos.DBOSClient`` for you — pointed at the
        namespace's schema, with the JSON serializer — so the namespace is
        stated exactly once and you never touch ``dbos_system_schema``. The
        returned client owns that DBOSClient; :py:meth:`close` (or
        ``async with``) disposes it. For full control over the DBOSClient
        (custom engine/pool), build it yourself and use the constructor.
        """
        dbos_client = await asyncio.to_thread(
            DBOSClient,
            system_database_url=system_database_url,
            dbos_system_schema=namespace_schema(namespace),
            serializer=TEMPORAL_SERIALIZER,
        )
        client = cls(
            dbos_client,
            data_converter=data_converter,
            interceptors=interceptors,
            default_workflow_query_reject_condition=default_workflow_query_reject_condition,
        )
        client._owns_dbos_client = True
        return client

    async def close(self) -> None:
        """Dispose the underlying DBOSClient if this client built it (via
        :py:meth:`connect`). A no-op for a caller-supplied DBOSClient — that one
        is the caller's to close."""
        if self._owns_dbos_client:
            await asyncio.to_thread(self._dbos_client.destroy)

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

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
        _with_start_update: Optional[Tuple[Any, str, str]] = None,
        **unsupported: Any,
    ) -> "WorkflowHandle":
        """Start a workflow and return its handle.

        Honored: arg/args, id, task_queue, run_timeout, all reuse/conflict
        policies, start_delay, start_signal, retry_policy (workflows do not
        retry by default, matching Temporal), and cron_schedule (each run
        starts at the next cron occurrence after the previous run closes;
        runs are chained like continue-as-new runs).
        """
        for key, value in {
            "execution_timeout": execution_timeout,
            "task_timeout": task_timeout,
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

        input = StartWorkflowInput(
            workflow=_workflow_type_name(workflow),
            args=_resolve_args(arg, args),
            id=id,
            task_queue=task_queue,
            execution_timeout=execution_timeout,
            run_timeout=run_timeout,
            task_timeout=task_timeout,
            id_reuse_policy=id_reuse_policy,
            id_conflict_policy=id_conflict_policy,
            retry_policy=retry_policy,
            cron_schedule=cron_schedule,
            memo=memo,
            search_attributes=search_attributes,
            start_delay=start_delay,
            headers={},
            start_signal=start_signal,
            start_signal_args=start_signal_args,
            static_summary=static_summary,
            static_details=static_details,
            ret_type=_result_type_for(workflow, result_type),
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
            request_eager_start=request_eager_start,
            priority=priority,
            callbacks=[],
            links=[],
            request_id=request_id,
            versioning_override=None,
            with_start_update=_with_start_update,
        )
        return await self._impl.start_workflow(input)

    async def _start_workflow_impl(self, input: StartWorkflowInput) -> "WorkflowHandle":
        """Root of the ``start_workflow`` outbound chain (DESIGN §6.8): the
        actual enqueue, reading the (possibly interceptor-modified) input."""
        workflow_args = input.args
        type_name = input.workflow
        result_type = input.ret_type
        id = input.id
        task_queue = input.task_queue
        run_timeout = input.run_timeout
        retry_policy = input.retry_policy
        cron_schedule = input.cron_schedule
        id_conflict_policy = input.id_conflict_policy
        id_reuse_policy = input.id_reuse_policy
        start_delay = input.start_delay
        start_signal = input.start_signal
        start_signal_args = input.start_signal_args
        memo = input.memo
        search_attributes = input.search_attributes

        if not task_queue or not isinstance(task_queue, str):
            # Without this, a None/empty queue name would enqueue a workflow
            # no worker can ever dequeue — a silent black hole.
            raise ValueError("task_queue must be a non-empty string")
        ids.validate_workflow_id(id)
        if start_delay is not None and start_delay < timedelta(0):
            # Matching temporalio's client-side check.
            raise ValueError("start_delay must be non-negative")

        meta = RunMeta()
        if retry_policy is not None:
            retry_policy._validate()
            meta.retry_policy = serialize_retry_policy(retry_policy)
        if run_timeout is not None:
            # Carried so chain successors (retries, cron, continue-as-new)
            # each get a fresh per-run timeout — without it, DBOS propagates
            # the closing run's *absolute* deadline to the runs it enqueues.
            meta.run_timeout = run_timeout.total_seconds()
        if cron_schedule:
            # Validated up front so a bad expression fails the start, not
            # the first chain hop. The first run is created immediately but
            # fires at the next cron occurrence (Temporal's first-task
            # backoff), so describe()/signals/result() work right away.
            _schedules.validate_cron(cron_schedule)
            if start_delay is not None:
                # DEVIATION (DEVIATIONS D19): our cron uses the enqueue delay
                # internally to back off run 0 to the first occurrence, so a
                # user start_delay can't ride alongside. temporalio accepts
                # the combination and silently ignores start_delay ("does not
                # work with cron_schedule"); we fail fast instead of swallowing
                # a behavior-changing parameter.
                raise ValueError(
                    "start_delay cannot be used together with cron_schedule"
                )
            meta.cron = cron_schedule
            start_delay = timedelta(
                seconds=_schedules.next_fire_delay(
                    cron_schedule, datetime.now(timezone.utc)
                )
            )

        # Memo + search attributes ride in the run envelope (for in-workflow
        # info()/memo() and chain propagation) and in the DBOS attributes column
        # (the durable, queryable copy that describe() reads). execute_workflow
        # and WithStartWorkflowOperation both funnel through here, so the
        # deprecated-dict-form warning is emitted once, at this chokepoint.
        _warn_on_deprecated_search_attributes(search_attributes)
        meta.attributes = await _attributes.encode_attributes(memo, search_attributes)
        # Interceptor headers (set by a client interceptor's start_workflow) ride
        # into the run as ExecuteWorkflowInput.headers (DEVIATIONS D24).
        meta.headers = (await conversion.encode_headers(input.headers)) or None

        # Messages to deliver with the start — Temporal's atomic
        # signal-/update-with-start (DEVIATIONS D7): the start signal and/or an
        # update request, each ``(envelope, topic, idempotency_key)``. Built
        # before conflict resolution so they ride whichever path the start
        # takes: bundled into the enqueue transaction on a fresh start, or sent
        # to the run we attach to under USE_EXISTING.
        with_start_msgs: List[Tuple[Any, str, Optional[str]]] = []
        if start_signal is not None:
            with_start_msgs.append(
                (
                    inbox.signal_envelope(
                        start_signal,
                        await conversion.encode_values(start_signal_args),
                        headers=await conversion.encode_headers(input.headers),
                    ),
                    inbox.INBOX_TOPIC,
                    None,
                )
            )
        if input.with_start_update is not None:
            with_start_msgs.append(input.with_start_update)

        current = await self._current_run(id)
        run_index = 0
        if current is not None:
            current_index, current_status = current
            if _status.is_open(current_status.status):
                # Conflict policies (vs a RUNNING run). There is an inherent
                # TOCTOU window here, accepted for v1 (DESIGN §6.4).
                if id_conflict_policy == WorkflowIDConflictPolicy.USE_EXISTING:
                    # Attaching to the running run: deliver the with-start
                    # messages to it (there is no enqueue to bundle with).
                    for env, topic, idem in with_start_msgs:
                        await self._dbos_client.send_async(
                            current_status.workflow_id, env, topic, idempotency_key=idem
                        )
                    return WorkflowHandle(
                        self,
                        id,
                        run_id=current_status.workflow_id,
                        result_type=result_type,
                    )
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
        if meta.attributes is not None:
            options["attributes"] = meta.attributes
        payload = wrap_input(await conversion.encode_values(workflow_args), meta)
        # Fresh start: bundle the enqueue and any with-start messages into one
        # system-database transaction so a crash can't leave the run started
        # without them. The commit runs in a thread, mirroring how
        # enqueue_async/send_async bridge to the sync DBOS client.
        if not with_start_msgs:
            await self._dbos_client.enqueue_async(options, payload)
        else:
            dbos_client = self._dbos_client

            def _enqueue_with_messages() -> None:
                with dbos_client._sys_db.engine.begin() as conn:
                    dbos_client.enqueue_in_transaction(conn, options, payload)
                    for env, topic, idem in with_start_msgs:
                        dbos_client.send_in_transaction(conn, dbos_id, env, topic, idem)

            await asyncio.to_thread(_enqueue_with_messages)
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
            result_type=result_type,
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
            result_type=result_type,
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
            workflow_id,
            run_id=run_id,
            first_execution_run_id=first_execution_run_id,
            result_type=_result_type_for(workflow, None),
        )

    # ------------------------------------------------------------------
    # Visibility (DESIGN §6.2)
    # ------------------------------------------------------------------

    def list_workflows(
        self,
        query: Optional[str] = None,
        *,
        limit: Optional[int] = None,
        page_size: int = 1000,
        next_page_token: Optional[bytes] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowExecutionAsyncIterator:
        """List workflows matching a visibility ``query`` (a Temporal-style
        filter string; see :mod:`temporal_dbos._internal.visibility` for the
        supported subset). Newest-first. As in temporalio, no request is made
        until the first iteration, so a bad query raises on first ``__anext__``.

        Each run-chain link (continue-as-new / workflow-retry / cron hop) is a
        separate row, keyed by its run id (decision §10.3).
        """
        _ignore_rpc_options("list_workflows", rpc_metadata, rpc_timeout)
        return WorkflowExecutionAsyncIterator(
            self,
            query=query,
            page_size=page_size,
            limit=limit,
            next_page_token=next_page_token,
        )

    async def count_workflows(
        self,
        query: Optional[str] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowExecutionCount:
        """Count workflows matching a visibility ``query``, optionally with a
        trailing ``GROUP BY WorkflowType``.

        Counting is **aggregate-only**: it runs entirely through DBOS's
        server-side ``get_workflow_aggregates`` (``COUNT`` + ``GROUP BY``).
        Queries it cannot express there are rejected with a
        :class:`~temporal_dbos._internal.visibility.VisibilityQueryError` rather
        than silently scanning rows — namely filters on exact ``WorkflowId``, a
        search attribute, ``WorkflowType !=``, or an ERROR-family
        ``ExecutionStatus`` (Failed/Canceled/TimedOut/ContinuedAsNew, which DBOS
        stores under a single ``ERROR`` status), and ``GROUP BY ExecutionStatus``
        (distinguishing those error states needs each workflow's recorded
        outcome). Use :py:meth:`list_workflows` to enumerate those.
        """
        _ignore_rpc_options("count_workflows", rpc_metadata, rpc_timeout)
        parsed = _visibility.parse_query(query)
        sys_db: Any = getattr(self._dbos_client, "_sys_db", None)
        if sys_db is None:
            raise RuntimeError(
                "count_workflows requires DBOS system-database access "
                "(the wrapped DBOSClient exposes no _sys_db)"
            )

        if not parsed.aggregate_eligible() or parsed.post_filter() is not None:
            raise _visibility.VisibilityQueryError(
                "count_workflows runs only what DBOS's aggregate operator can "
                "express: it cannot filter by exact WorkflowId, a search "
                "attribute, WorkflowType !=, or an ERROR-family ExecutionStatus "
                "(Failed/Canceled/TimedOut/ContinuedAsNew, stored as one ERROR "
                "status). Narrow the query or enumerate with list_workflows."
            )
        if parsed.group_by == "ExecutionStatus":
            raise _visibility.VisibilityQueryError(
                "count_workflows cannot GROUP BY ExecutionStatus: DBOS stores "
                "Failed/Canceled/TimedOut/ContinuedAsNew under a single ERROR "
                "status, so distinguishing them would require reading each "
                "workflow's outcome. GROUP BY WorkflowType is supported."
            )

        # Group by name in every path so we can count only user Temporal
        # workflows (``wf:{type}``) and exclude DBOS plumbing rows
        # (``__temporal_activity`` / ``__temporal_schedule_fire``), which the
        # aggregate operator has no other way to filter out.
        rows = await self._count_aggregate(
            sys_db, "group_by_name", parsed.aggregate_filter_kwargs()
        )
        tallies: Dict[str, int] = {}
        for r in rows:
            name = r["group"].get("name") or ""
            if not name.startswith("wf:"):
                continue
            key = name[3:]
            tallies[key] = tallies.get(key, 0) + (r["count"] or 0)

        if parsed.group_by is None:
            return WorkflowExecutionCount(count=sum(tallies.values()), groups=[])

        # GROUP BY WorkflowType (the only cleanly-aggregatable grouping).
        groups = [
            WorkflowExecutionCountAggregationGroup(count=c, group_values=[v])
            for v, c in sorted(tallies.items())
        ]
        return WorkflowExecutionCount(count=sum(tallies.values()), groups=groups)

    async def _count_aggregate(
        self, sys_db: Any, group_flag: str, filter_kwargs: Mapping[str, Any]
    ) -> List[Any]:
        """One ``get_workflow_aggregates`` COUNT call grouped by ``group_flag``
        (run off-loop since the operator is synchronous)."""
        return await asyncio.to_thread(
            lambda: sys_db.get_workflow_aggregates(
                select_count=True, **{group_flag: True}, **filter_kwargs
            )
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
        ``wait_for_stage``. The update rides the start: on a *fresh* run the
        start enqueue and the update request commit in one system-database
        transaction (Temporal's atomic update-with-start, DEVIATIONS.md D7); on
        a USE_EXISTING attach it is sent to the already-running run as part of
        the start. The request is routed through the update outbound
        interceptors first, so their modifications apply on both paths.
        """
        op = start_workflow_operation
        if op._used:
            raise RuntimeError("WithStartWorkflowOperation cannot be reused")
        op._used = True
        # Route through the update interceptor chain; its terminal
        # (_start_update_impl, with ``with_start_op`` set) builds the
        # post-interceptor envelope and performs the start that delivers it,
        # then awaits the reply. The start half still runs the start
        # interceptors. op._handle is set by the terminal.
        return await self._impl.start_workflow_update(
            StartWorkflowUpdateInput(
                id=op._start_kwargs["id"],
                run_id=None,
                first_execution_run_id=None,
                update_id=id,
                update=_update_name(update),
                args=_resolve_args(arg, args),
                wait_for_stage=wait_for_stage,
                headers={},
                ret_type=_ref_ret_type(update, result_type),
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                with_start_op=op,
            )
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
    # Schedules (DESIGN §6.7)
    # ------------------------------------------------------------------

    async def create_schedule(
        self,
        id: str,
        schedule: Schedule,
        *,
        trigger_immediately: bool = False,
        backfill: Sequence[ScheduleBackfill] = [],
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[Any] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> ScheduleHandle:
        """Create a schedule and return its handle (DESIGN §6.7).

        The schedule's ``ScheduleSpec`` is compiled to a cron expression and
        backed by a DBOS schedule that fires a generic dispatcher; ``memo`` and
        ``search_attributes`` are not stored yet (debug-logged)."""
        if memo is not None or search_attributes is not None:
            logger.debug("create_schedule: ignoring unsupported memo/search_attributes")
        _ignore_rpc_options("create_schedule", rpc_metadata, rpc_timeout)
        return await self._impl.create_schedule(
            CreateScheduleInput(
                id=id,
                schedule=schedule,
                trigger_immediately=trigger_immediately,
                backfill=backfill,
                memo=memo,
                search_attributes=search_attributes,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def _create_schedule_impl(self, input: CreateScheduleInput) -> ScheduleHandle:
        await _schedule.create_schedule_row(
            self,
            input.id,
            input.schedule,
            trigger_immediately=input.trigger_immediately,
            backfill=input.backfill,
        )
        return ScheduleHandle(self, input.id)

    def get_schedule_handle(self, id: str) -> ScheduleHandle:
        """Get a handle for a schedule by id (does not verify existence)."""
        return ScheduleHandle(self, id)

    async def list_schedules(
        self,
        query: Optional[str] = None,
        *,
        page_size: int = 1000,
        next_page_token: Optional[bytes] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> ScheduleAsyncIterator:
        """List schedules. The visibility ``query`` filter is not supported yet
        (debug-logged); all temporal-dbos schedules are returned."""
        if query is not None:
            logger.debug("list_schedules: ignoring unsupported query filter")
        _ignore_rpc_options("list_schedules", rpc_metadata, rpc_timeout)
        return await self._impl.list_schedules(
            ListSchedulesInput(
                page_size=page_size,
                next_page_token=next_page_token,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                query=query,
            )
        )

    async def _list_schedules_impl(
        self, input: ListSchedulesInput
    ) -> ScheduleAsyncIterator:
        rows = await self._dbos_client.list_schedules_async()
        page = [
            _schedule._list_description_from_row(row)
            for row in rows
            if row.get("workflow_name") == _schedule.SCHEDULE_FIRE_WORKFLOW
        ]
        return ScheduleAsyncIterator(page)

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
        result_type: Optional[type] = None,
    ) -> None:
        self._client = client
        self._id = id
        self._run_id = run_id
        self._result_run_id = result_run_id
        self._first_execution_run_id = first_execution_run_id
        self._result_type = result_type

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
                raw = await handle.get_result()
                return await conversion.decode_value(raw, self._result_type)
            except SerializedContinueAsNew as marker:
                new_run_id: str = marker.envelope["new_run_id"]
                if not follow_runs:
                    raise WorkflowContinuedAsNewError(new_run_id) from None
                dbos_id = new_run_id
                continue
            except SerializedWorkflowFailure as failure:
                # A failed run with a successor (workflow retry, cron
                # continuation) is followed like temporalio follows
                # new_execution_run_id on the failure event; without
                # follow_runs (or a successor) the failure surfaces.
                successor = failure.envelope.get("new_run_id")
                if follow_runs and successor is not None:
                    dbos_id = successor
                    continue
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
        await self._client._impl.signal_workflow(
            SignalWorkflowInput(
                id=self._id,
                run_id=self._run_id,
                signal=_signal_name(signal),
                args=_resolve_args(arg, args),
                headers={},
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def _signal_impl(self, input: SignalWorkflowInput) -> None:
        encoded = await conversion.encode_values(input.args)
        await self._client._dbos_client.send_async(
            await self._target(),
            inbox.signal_envelope(
                input.signal,
                encoded,
                headers=await conversion.encode_headers(input.headers),
            ),
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
        return await self._client._impl.query_workflow(
            QueryWorkflowInput(
                id=self._id,
                run_id=self._run_id,
                query=_query_name(query),
                args=_resolve_args(arg, args),
                reject_condition=reject_condition,
                headers={},
                ret_type=_ref_ret_type(query, result_type),
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def _query_impl(self, input: QueryWorkflowInput) -> Any:
        condition = (
            input.reject_condition or self._client._default_query_reject_condition
        )
        # Read status once: it gates the reject_condition and decides whether the
        # query needs a rehydrate replay (closed workflow). Read directly rather
        # than via describe() so a describe_workflow interceptor is not invoked
        # as a side effect of a query. (No server arbiter, DEVIATIONS D7 family:
        # the status read and the query send are not atomic.) A missing run
        # surfaces as a query failure, not a bare RuntimeError.
        try:
            target = await self._target()
            raw = await self._client._status_of(target)
        except RuntimeError as err:
            raise WorkflowQueryFailedError(str(err)) from err
        status = _status.to_execution_status(raw.status, error=raw.error)
        if condition is not None and condition != QueryRejectCondition.NONE:
            rejected = (
                status != WorkflowExecutionStatus.RUNNING
                if condition == QueryRejectCondition.NOT_OPEN
                else status != WorkflowExecutionStatus.COMPLETED
            )
            if rejected:
                raise WorkflowQueryRejectedError(status)
        request_id = str(uuid_mod.uuid4())
        client = self._client._dbos_client
        timeout = input.rpc_timeout.total_seconds() if input.rpc_timeout else 60.0
        envelope = inbox.query_envelope(
            input.query,
            await conversion.encode_values(input.args),
            request_id,
            headers=await conversion.encode_headers(input.headers),
        )
        if status == WorkflowExecutionStatus.RUNNING:
            await client.send_async(target, envelope, inbox.INBOX_TOPIC)
            reply = await self._client._await_reply_event(
                self._id, target, inbox.query_result_key(request_id), timeout
            )
        elif status in (
            WorkflowExecutionStatus.COMPLETED,
            WorkflowExecutionStatus.FAILED,
            WorkflowExecutionStatus.CANCELED,
        ):
            # Query on a closed workflow: rehydrate by replay — fork the run to
            # reconstruct its final state, serve the query against it, discard
            # the scratch run (resolves README deviation #2 / Temporal serving
            # queries after completion). Requires a worker in this process (the
            # fork executes locally and consults the in-process rehydrate guard).
            reply = await self._rehydrate_query(target, envelope, request_id, timeout)
        else:
            # TERMINATED (native kill, only partial checkpoints), TIMED_OUT, and
            # CONTINUED_AS_NEW cannot be faithfully replayed to reconstruct a
            # queryable final state — fail clearly rather than spin up a fork
            # that diverges (DEVIATIONS D27).
            raise WorkflowQueryFailedError(
                f"cannot query a workflow in state {status.name}: rehydrate-by-"
                "replay supports COMPLETED/FAILED/CANCELED runs only "
                "(see DEVIATIONS D27)"
            )
        if reply is None:
            raise WorkflowQueryFailedError(f"query did not complete within {timeout}s")
        if reply["status"] == "completed":
            return await conversion.decode_value(reply["result"], input.ret_type)
        raise WorkflowQueryFailedError(str(deserialize_failure(reply["failure"])))

    async def _rehydrate_query(
        self, target: str, envelope: Any, request_id: str, timeout: float
    ) -> Optional[Any]:
        """Answer a query on a closed workflow by replaying it: fork the run one
        step past its last checkpoint (copying every recorded step), let the
        forked run replay to its final state and then serve this one query
        against that reconstructed state, then discard the scratch run."""
        client = self._client._dbos_client
        steps = await client.list_workflow_steps_async(target)
        # The fork-one-past-horizon convention + guard registration are shared
        # with the verification replayer (replay.start_replay_fork).
        scratch_handle = await _replay.start_replay_fork(
            client, target, steps, mode="rehydrate"
        )
        scratch_id = scratch_handle.get_workflow_id()
        reply_key = inbox.query_result_key(request_id)
        try:
            await client.send_async(scratch_id, envelope, inbox.INBOX_TOPIC)
            reply = await self._await_rehydrate_reply(
                scratch_handle, scratch_id, reply_key, timeout
            )
            if reply is None:
                # The fork reached a terminal state without serving the query:
                # the reconstruction diverged (the workflow's code changed since
                # it ran), or no worker for this type is running in *this*
                # process to drive the rehydrate (DEVIATIONS D27).
                raise WorkflowQueryFailedError(
                    "rehydrate-by-replay produced no query reply: the workflow's "
                    "code may have changed since it ran, or no worker for this "
                    "type is running in the querying process (DEVIATIONS D27)"
                )
            return reply
        finally:
            # Stop the scratch run serving and wait for it to settle BEFORE
            # unregistering the guard, so the guard stays active for the whole
            # time the fork is executing — a fork that is still replaying must
            # never run a real op past the horizon while unguarded. Cancel it if
            # it overruns the settle window, then unregister and delete.
            try:
                await client.send_async(
                    scratch_id, inbox.rehydrate_stop_envelope(), inbox.INBOX_TOPIC
                )
            except Exception:  # noqa: BLE001 — best-effort
                pass
            try:
                await asyncio.wait_for(
                    scratch_handle.get_result(polling_interval_sec=0.05),
                    timeout=_replay.REHYDRATE_SETTLE_SECONDS,
                )
            except asyncio.TimeoutError:
                try:
                    await client.cancel_workflow_async(scratch_id)
                except Exception:  # noqa: BLE001 — best-effort
                    pass
            except Exception:  # noqa: BLE001 — the run reached a terminal state
                pass
            _replay.unregister_guard(scratch_id)
            try:
                # delete_children stays False: the rehydrated fork starts no
                # real children (they replay from checkpoints), so it owns none
                # — and we must never delete the source run's subtree.
                await client.delete_workflow_async(scratch_id, delete_children=False)
            except Exception:  # noqa: BLE001 — cleanup is best-effort
                logger.warning(
                    "query rehydrate: failed to delete scratch run %s", scratch_id
                )

    async def _await_rehydrate_reply(
        self, scratch_handle: Any, scratch_id: str, reply_key: str, timeout: float
    ) -> Optional[Any]:
        """Wait for a rehydrate fork's query reply, but stop as soon as the fork
        reaches a terminal state without one — so a diverged (code-changed) or
        non-serving fork fails fast instead of blocking the full ``timeout``.

        A faithful rehydrate serves the reply and then parks for the stop signal
        (its result never resolves here), so the reply wins the race; a fork that
        never serves (diverged past the horizon, or completed with no in-process
        worker) reaches a terminal result with no reply, so the result wins and
        we return None.
        """
        reply_task = asyncio.ensure_future(
            self._client._await_reply_event(scratch_id, scratch_id, reply_key, timeout)
        )
        result_task = asyncio.ensure_future(
            scratch_handle.get_result(polling_interval_sec=0.05)
        )
        try:
            done, _pending = await asyncio.wait(
                {reply_task, result_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if reply_task in done and reply_task.exception() is None:
                return reply_task.result()  # reply payload, or None on its timeout
            return None  # the fork terminated without serving a reply
        finally:
            for task in (reply_task, result_task):
                if not task.done():
                    task.cancel()
            # Drain both so a cancelled wait or a fork failure is not an
            # "exception was never retrieved" warning.
            for task in (reply_task, result_task):
                try:
                    await task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass

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
        return await self._client._impl.start_workflow_update(
            StartWorkflowUpdateInput(
                id=self._id,
                run_id=self._run_id,
                first_execution_run_id=self._first_execution_run_id,
                update_id=id,
                update=_update_name(update),
                args=_resolve_args(arg, args),
                wait_for_stage=wait_for_stage,
                headers={},
                ret_type=_ref_ret_type(update, result_type),
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def _start_update_impl(
        self, input: StartWorkflowUpdateInput
    ) -> WorkflowUpdateHandle:
        if input.wait_for_stage not in (
            WorkflowUpdateStage.ACCEPTED,
            WorkflowUpdateStage.COMPLETED,
        ):
            raise ValueError("Admitted wait stage not supported")
        update_id = input.update_id or str(uuid_mod.uuid4())
        timeout = input.rpc_timeout.total_seconds() if input.rpc_timeout else 60.0
        # Built from the (post-interceptor) input, so interceptor modifications
        # ride along whether the update is delivered atomically with a fresh
        # start or sent to an already-running run.
        envelope = inbox.update_envelope(
            input.update,
            await conversion.encode_values(input.args),
            update_id,
            headers=await conversion.encode_headers(input.headers),
        )
        op = input.with_start_op
        if op is not None:
            # update-with-start: the start delivers the update — atomically in
            # the enqueue transaction on a fresh run, or to the attached run
            # under USE_EXISTING (see Client._start_workflow_impl).
            start_args = op._start_kwargs["args"]
            start_kwargs = {k: v for k, v in op._start_kwargs.items() if k != "args"}
            op._handle = await self._client.start_workflow(
                op._workflow,
                args=start_args,
                **start_kwargs,
                _with_start_update=(envelope, inbox.INBOX_TOPIC, update_id),
            )
            target = await op._handle._target()
        else:
            target = await self._target()
            await self._client._dbos_client.send_async(
                target, envelope, inbox.INBOX_TOPIC, idempotency_key=update_id
            )
        handle = WorkflowUpdateHandle(
            self._client,
            update_id,
            self._id,
            workflow_run_id=target,
            result_type=input.ret_type,
        )
        if input.wait_for_stage == WorkflowUpdateStage.ACCEPTED:
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
        return await self._client._impl.describe_workflow(
            DescribeWorkflowInput(
                id=self._id,
                run_id=self._run_id,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def _describe_impl(
        self, input: DescribeWorkflowInput
    ) -> WorkflowExecutionDescription:
        dbos_id = await self._target()
        status = await self._client._status_of(dbos_id)
        description = _execution_from_status(
            status, WorkflowExecutionDescription, namespace=self._client._namespace
        )
        assert isinstance(description, WorkflowExecutionDescription)
        # describe() is bound to a specific Temporal workflow id; honor it over
        # the chain-base derived from the (possibly run-suffixed) DBOS id.
        object.__setattr__(description, "id", self._id)
        return description

    async def fetch_history(
        self,
        *,
        event_filter_type: Any = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowHistory:
        """Snapshot this run's recorded execution as a :class:`WorkflowHistory`
        for replay. Reads the run's DBOS status plus its step checkpoints
        (``list_workflow_steps``). ``event_filter_type`` has no analog (we have
        no event log) and is accepted/ignored.

        Unlike the interpreter's in-workflow step read (which must hop to an
        executor thread to stay live, see ``interpreter.execute``), this runs on
        a plain client with no DBOS workflow context, so the async call is safe.

        Resolves the run the same way :py:meth:`describe` does (the bound run, or
        the chain's current run) rather than anchoring on the first run, so the
        same id snapshots the same run regardless of how the handle was obtained.
        """
        _ignore_rpc_options("fetch_history", rpc_metadata, rpc_timeout)
        if event_filter_type is not None:
            logger.debug("fetch_history: ignoring event_filter_type")
        dbos_id = await self._target()
        status = await self._client._status_of(dbos_id)
        steps = await self._client._dbos_client.list_workflow_steps_async(dbos_id)
        workflow_type = status.name or ""
        if workflow_type.startswith("wf:"):
            workflow_type = workflow_type[3:]
        return WorkflowHistory(
            workflow_id=self._id,
            run_id=dbos_id,
            workflow_type=workflow_type,
            input=status.input,
            recorded_steps=list(steps),
            status=_status.to_execution_status(status.status, error=status.error),
            attributes=status.attributes or {},
            app_version=status.app_version,
        )

    async def fetch_history_events(self, **kwargs: Any) -> Any:
        """Not supported: temporal-dbos has no Temporal event history. Use
        :py:meth:`fetch_history`, which returns a DBOS-step-derived
        :class:`WorkflowHistory`."""
        raise NotImplementedError(
            "temporal-dbos has no Temporal event history; use "
            "WorkflowHandle.fetch_history() for a DBOS-step-derived history"
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
        swallow the cancel and complete normally. Raises if the targeted
        run is already closed (as in Temporal); the status check is
        client-side, so a tiny race window remains (D7 family).
        """
        _ignore_rpc_options("cancel", rpc_metadata, rpc_timeout)
        await self._client._impl.cancel_workflow(
            CancelWorkflowInput(
                id=self._id,
                run_id=self._run_id,
                first_execution_run_id=self._first_execution_run_id,
                reason=reason,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def _cancel_impl(self, input: CancelWorkflowInput) -> None:
        target = await self._target()
        status = await self._client._status_of(target)
        mapped = _status.to_execution_status(status.status, error=status.error)
        if mapped != WorkflowExecutionStatus.RUNNING:
            raise RuntimeError(
                f"Workflow run already closed: {target!r} ({mapped.name})"
            )
        await self._client._dbos_client.send_async(
            target, inbox.cancel_envelope(input.reason), inbox.INBOX_TOPIC
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
        await self._client._impl.terminate_workflow(
            TerminateWorkflowInput(
                id=self._id,
                run_id=self._run_id,
                first_execution_run_id=self._first_execution_run_id,
                args=list(args),
                reason=reason,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def _terminate_impl(self, input: TerminateWorkflowInput) -> None:
        if input.args or input.reason:
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


class _ClientOutbound(OutboundInterceptor):
    """Root of the client outbound interceptor chain (DESIGN §6.8): performs
    the actual DBOS operations, reading the (possibly interceptor-modified)
    ``*Input``. Verbs whose work lives on a handle reconstruct the handle from
    the input's identity and call its ``_<verb>_impl``; the rest delegate to
    the matching ``Client._<verb>_impl``.
    """

    def __init__(self, client: "Client") -> None:
        # Chain root: there is no ``next`` to delegate to — every verb is
        # overridden — so we intentionally do not call super().__init__.
        self._client = client

    # --- Workflow calls ---

    async def start_workflow(self, input: StartWorkflowInput) -> "WorkflowHandle":
        return await self._client._start_workflow_impl(input)

    async def cancel_workflow(self, input: CancelWorkflowInput) -> None:
        await WorkflowHandle(self._client, input.id, run_id=input.run_id)._cancel_impl(
            input
        )

    async def describe_workflow(
        self, input: DescribeWorkflowInput
    ) -> "WorkflowExecutionDescription":
        return await WorkflowHandle(
            self._client, input.id, run_id=input.run_id
        )._describe_impl(input)

    async def query_workflow(self, input: QueryWorkflowInput) -> Any:
        return await WorkflowHandle(
            self._client, input.id, run_id=input.run_id
        )._query_impl(input)

    async def signal_workflow(self, input: SignalWorkflowInput) -> None:
        await WorkflowHandle(self._client, input.id, run_id=input.run_id)._signal_impl(
            input
        )

    async def terminate_workflow(self, input: TerminateWorkflowInput) -> None:
        await WorkflowHandle(
            self._client, input.id, run_id=input.run_id
        )._terminate_impl(input)

    async def start_workflow_update(
        self, input: StartWorkflowUpdateInput
    ) -> "WorkflowUpdateHandle":
        return await WorkflowHandle(
            self._client, input.id, run_id=input.run_id
        )._start_update_impl(input)

    # --- Async activity calls ---

    async def heartbeat_async_activity(
        self, input: HeartbeatAsyncActivityInput
    ) -> None:
        await AsyncActivityHandle(self._client, input.id_or_token)._heartbeat_impl(
            input
        )

    async def complete_async_activity(self, input: CompleteAsyncActivityInput) -> None:
        await AsyncActivityHandle(self._client, input.id_or_token)._complete_impl(input)

    async def fail_async_activity(self, input: FailAsyncActivityInput) -> None:
        await AsyncActivityHandle(self._client, input.id_or_token)._fail_impl(input)

    async def report_cancellation_async_activity(
        self, input: ReportCancellationAsyncActivityInput
    ) -> None:
        await AsyncActivityHandle(
            self._client, input.id_or_token
        )._report_cancellation_impl(input)

    # --- Schedule calls ---

    async def create_schedule(self, input: CreateScheduleInput) -> "ScheduleHandle":
        return await self._client._create_schedule_impl(input)

    async def list_schedules(
        self, input: ListSchedulesInput
    ) -> "ScheduleAsyncIterator":
        return await self._client._list_schedules_impl(input)

    async def backfill_schedule(self, input: BackfillScheduleInput) -> None:
        await ScheduleHandle(self._client, input.id)._backfill_impl(input)

    async def delete_schedule(self, input: DeleteScheduleInput) -> None:
        await ScheduleHandle(self._client, input.id)._delete_impl(input)

    async def describe_schedule(
        self, input: DescribeScheduleInput
    ) -> "ScheduleDescription":
        return await ScheduleHandle(self._client, input.id)._describe_impl(input)

    async def pause_schedule(self, input: PauseScheduleInput) -> None:
        await ScheduleHandle(self._client, input.id)._pause_impl(input)

    async def trigger_schedule(self, input: TriggerScheduleInput) -> None:
        await ScheduleHandle(self._client, input.id)._trigger_impl(input)

    async def unpause_schedule(self, input: UnpauseScheduleInput) -> None:
        await ScheduleHandle(self._client, input.id)._unpause_impl(input)

    async def update_schedule(self, input: UpdateScheduleInput) -> None:
        await ScheduleHandle(self._client, input.id)._update_impl(input)

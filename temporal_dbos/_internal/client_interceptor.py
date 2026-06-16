"""Client-side interceptors, mirroring ``temporalio.client`` interceptors
(``temporalio/client/_interceptor.py``).

This is the Phase-3 surface. The classes are re-exported from
:py:mod:`temporal_dbos.client` so user code extends
``temporal_dbos.client.Interceptor`` exactly as it would
``temporalio.client.Interceptor``.

The ``*Input`` dataclasses are copied field-for-field from the SDK (DESIGN
§6.8) so signature parity holds; fields temporal_dbos does not act on (e.g.
``callbacks``, ``links``, ``request_id``, ``versioning_override``, ``priority``,
``rpc_metadata``/``rpc_timeout``, ``data_converter_override``) are carried but
inert. ``headers`` is *not* inert: a header set on ``start_workflow`` /
``signal_workflow`` / ``query_workflow`` / ``start_workflow_update`` propagates
into the workflow as ``ExecuteWorkflowInput.headers`` / the matching
``Handle*Input.headers`` (header-based context propagation, DEVIATIONS D24).
Annotations use our own types or ``Any`` (the parity test checks parameter
names/kind/default/order, not annotations).

``OutboundInterceptor`` exposes only the verbs temporal_dbos actually routes
through the chain — Nexus, worker build-id, workflow history-event fetching,
and the distributed activity-as-RPC verbs are unsupported and intentionally
absent (no silent no-op overrides). ``Client.start_update_with_start_workflow``
is intercepted via its constituent ``start_workflow`` + ``start_workflow_update``
calls rather than as its own outbound verb.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Mapping,
    Optional,
    Sequence,
)

from ..common import (
    QueryRejectCondition,
    RetryPolicy,
    WorkflowIDConflictPolicy,
    WorkflowIDReusePolicy,
)

if TYPE_CHECKING:
    from .._schedule import (
        ScheduleAsyncIterator,
        ScheduleDescription,
        ScheduleHandle,
    )
    from ..client import (
        AsyncActivityHandle,
        WorkflowExecutionDescription,
        WorkflowHandle,
        WorkflowUpdateHandle,
        WorkflowUpdateStage,
    )

__all__ = [
    "Interceptor",
    "OutboundInterceptor",
    "StartWorkflowInput",
    "CancelWorkflowInput",
    "DescribeWorkflowInput",
    "QueryWorkflowInput",
    "SignalWorkflowInput",
    "TerminateWorkflowInput",
    "StartWorkflowUpdateInput",
    "HeartbeatAsyncActivityInput",
    "CompleteAsyncActivityInput",
    "FailAsyncActivityInput",
    "ReportCancellationAsyncActivityInput",
    "CreateScheduleInput",
    "ListSchedulesInput",
    "BackfillScheduleInput",
    "DeleteScheduleInput",
    "DescribeScheduleInput",
    "PauseScheduleInput",
    "TriggerScheduleInput",
    "UnpauseScheduleInput",
    "UpdateScheduleInput",
]


# --------------------------------------------------------------------------
# Workflow inputs
# --------------------------------------------------------------------------


@dataclass
class StartWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.start_workflow`."""

    workflow: str
    args: Sequence[Any]
    id: str
    task_queue: str
    execution_timeout: Optional[timedelta]
    run_timeout: Optional[timedelta]
    task_timeout: Optional[timedelta]
    id_reuse_policy: WorkflowIDReusePolicy
    id_conflict_policy: WorkflowIDConflictPolicy
    retry_policy: Optional[RetryPolicy]
    cron_schedule: str
    memo: Optional[Mapping[str, Any]]
    search_attributes: Optional[Any]
    start_delay: Optional[timedelta]
    headers: Mapping[str, Any]
    start_signal: Optional[str]
    start_signal_args: Sequence[Any]
    static_summary: Optional[str]
    static_details: Optional[str]
    # Type may be absent
    ret_type: Optional[type]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]
    request_eager_start: bool
    priority: Any
    # The following options are experimental and unstable.
    callbacks: Sequence[Any]
    links: Sequence[Any]
    request_id: Optional[str]
    versioning_override: Optional[Any] = None


@dataclass
class CancelWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.cancel_workflow`."""

    id: str
    run_id: Optional[str]
    first_execution_run_id: Optional[str]
    reason: str
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]


@dataclass
class DescribeWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.describe_workflow`."""

    id: str
    run_id: Optional[str]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]


@dataclass
class QueryWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.query_workflow`."""

    id: str
    run_id: Optional[str]
    query: str
    args: Sequence[Any]
    reject_condition: Optional[QueryRejectCondition]
    headers: Mapping[str, Any]
    # Type may be absent
    ret_type: Optional[type]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]


@dataclass
class SignalWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.signal_workflow`."""

    id: str
    run_id: Optional[str]
    signal: str
    args: Sequence[Any]
    headers: Mapping[str, Any]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]


@dataclass
class TerminateWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.terminate_workflow`."""

    id: str
    run_id: Optional[str]
    first_execution_run_id: Optional[str]
    args: Sequence[Any]
    reason: Optional[str]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]


@dataclass
class StartWorkflowUpdateInput:
    """Input for :py:meth:`OutboundInterceptor.start_workflow_update`."""

    id: str
    run_id: Optional[str]
    first_execution_run_id: Optional[str]
    update_id: Optional[str]
    update: str
    args: Sequence[Any]
    wait_for_stage: "WorkflowUpdateStage"
    headers: Mapping[str, Any]
    ret_type: Optional[type]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]


# --------------------------------------------------------------------------
# Async activity inputs
# --------------------------------------------------------------------------


@dataclass
class HeartbeatAsyncActivityInput:
    """Input for :py:meth:`OutboundInterceptor.heartbeat_async_activity`."""

    id_or_token: Any
    details: Sequence[Any]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]
    data_converter_override: Optional[Any] = None


@dataclass
class CompleteAsyncActivityInput:
    """Input for :py:meth:`OutboundInterceptor.complete_async_activity`."""

    id_or_token: Any
    result: Optional[Any]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]
    data_converter_override: Optional[Any] = None


@dataclass
class FailAsyncActivityInput:
    """Input for :py:meth:`OutboundInterceptor.fail_async_activity`."""

    id_or_token: Any
    error: Exception
    last_heartbeat_details: Sequence[Any]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]
    data_converter_override: Optional[Any] = None


@dataclass
class ReportCancellationAsyncActivityInput:
    """Input for :py:meth:`OutboundInterceptor.report_cancellation_async_activity`."""

    id_or_token: Any
    details: Sequence[Any]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]
    data_converter_override: Optional[Any] = None


# --------------------------------------------------------------------------
# Schedule inputs
# --------------------------------------------------------------------------


@dataclass
class CreateScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.create_schedule`."""

    id: str
    schedule: Any
    trigger_immediately: bool
    backfill: Sequence[Any]
    memo: Optional[Mapping[str, Any]]
    search_attributes: Optional[Any]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]


@dataclass
class ListSchedulesInput:
    """Input for :py:meth:`OutboundInterceptor.list_schedules`."""

    page_size: int
    next_page_token: Optional[bytes]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]
    query: Optional[str] = None


@dataclass
class BackfillScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.backfill_schedule`."""

    id: str
    backfills: Sequence[Any]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]


@dataclass
class DeleteScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.delete_schedule`."""

    id: str
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]


@dataclass
class DescribeScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.describe_schedule`."""

    id: str
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]


@dataclass
class PauseScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.pause_schedule`."""

    id: str
    note: Optional[str]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]


@dataclass
class TriggerScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.trigger_schedule`."""

    id: str
    overlap: Optional[Any]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]


@dataclass
class UnpauseScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.unpause_schedule`."""

    id: str
    note: Optional[str]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]


@dataclass
class UpdateScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.update_schedule`."""

    id: str
    updater: Callable[..., Any]
    rpc_metadata: Mapping[str, Any]
    rpc_timeout: Optional[timedelta]


# --------------------------------------------------------------------------
# Interceptor base classes
# --------------------------------------------------------------------------


@dataclass
class Interceptor:
    """Interceptor for clients.

    This should be extended by any client interceptors. Pass instances to
    ``temporal_dbos.client.Client(..., interceptors=[...])``.
    """

    def intercept_client(self, next: OutboundInterceptor) -> OutboundInterceptor:
        """Method called for intercepting a client.

        Args:
            next: The underlying outbound interceptor this interceptor should
                delegate to.

        Returns:
            The new interceptor that will be called for each client call.
        """
        return next


class OutboundInterceptor:
    """OutboundInterceptor for intercepting client calls.

    This should be extended by any client outbound interceptors.
    """

    def __init__(self, next: OutboundInterceptor) -> None:
        """Create the outbound interceptor.

        Args:
            next: The next interceptor in the chain. The default implementation
                of all calls is to delegate to the next interceptor.
        """
        self.next = next

    # --- Workflow calls ---

    async def start_workflow(self, input: StartWorkflowInput) -> "WorkflowHandle":
        """Called for every :py:meth:`Client.start_workflow` call."""
        return await self.next.start_workflow(input)

    async def cancel_workflow(self, input: CancelWorkflowInput) -> None:
        """Called for every :py:meth:`WorkflowHandle.cancel` call."""
        await self.next.cancel_workflow(input)

    async def describe_workflow(
        self, input: DescribeWorkflowInput
    ) -> "WorkflowExecutionDescription":
        """Called for every :py:meth:`WorkflowHandle.describe` call."""
        return await self.next.describe_workflow(input)

    async def query_workflow(self, input: QueryWorkflowInput) -> Any:
        """Called for every :py:meth:`WorkflowHandle.query` call."""
        return await self.next.query_workflow(input)

    async def signal_workflow(self, input: SignalWorkflowInput) -> None:
        """Called for every :py:meth:`WorkflowHandle.signal` call."""
        await self.next.signal_workflow(input)

    async def terminate_workflow(self, input: TerminateWorkflowInput) -> None:
        """Called for every :py:meth:`WorkflowHandle.terminate` call."""
        await self.next.terminate_workflow(input)

    async def start_workflow_update(
        self, input: StartWorkflowUpdateInput
    ) -> "WorkflowUpdateHandle":
        """Called for every :py:meth:`WorkflowHandle.start_update` and
        :py:meth:`WorkflowHandle.execute_update` call."""
        return await self.next.start_workflow_update(input)

    # --- Async activity calls ---

    async def heartbeat_async_activity(
        self, input: HeartbeatAsyncActivityInput
    ) -> None:
        """Called for every :py:meth:`AsyncActivityHandle.heartbeat` call."""
        await self.next.heartbeat_async_activity(input)

    async def complete_async_activity(self, input: CompleteAsyncActivityInput) -> None:
        """Called for every :py:meth:`AsyncActivityHandle.complete` call."""
        await self.next.complete_async_activity(input)

    async def fail_async_activity(self, input: FailAsyncActivityInput) -> None:
        """Called for every :py:meth:`AsyncActivityHandle.fail` call."""
        await self.next.fail_async_activity(input)

    async def report_cancellation_async_activity(
        self, input: ReportCancellationAsyncActivityInput
    ) -> None:
        """Called for every :py:meth:`AsyncActivityHandle.report_cancellation` call."""
        await self.next.report_cancellation_async_activity(input)

    # --- Schedule calls ---

    async def create_schedule(self, input: CreateScheduleInput) -> "ScheduleHandle":
        """Called for every :py:meth:`Client.create_schedule` call."""
        return await self.next.create_schedule(input)

    async def list_schedules(
        self, input: ListSchedulesInput
    ) -> "ScheduleAsyncIterator":
        """Called for every :py:meth:`Client.list_schedules` call.

        (Async in temporal_dbos — our ``Client.list_schedules`` is async —
        unlike temporalio's synchronous outbound; invisible to parity.)"""
        return await self.next.list_schedules(input)

    async def backfill_schedule(self, input: BackfillScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.backfill` call."""
        await self.next.backfill_schedule(input)

    async def delete_schedule(self, input: DeleteScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.delete` call."""
        await self.next.delete_schedule(input)

    async def describe_schedule(
        self, input: DescribeScheduleInput
    ) -> "ScheduleDescription":
        """Called for every :py:meth:`ScheduleHandle.describe` call."""
        return await self.next.describe_schedule(input)

    async def pause_schedule(self, input: PauseScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.pause` call."""
        await self.next.pause_schedule(input)

    async def trigger_schedule(self, input: TriggerScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.trigger` call."""
        await self.next.trigger_schedule(input)

    async def unpause_schedule(self, input: UnpauseScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.unpause` call."""
        await self.next.unpause_schedule(input)

    async def update_schedule(self, input: UpdateScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.update` call."""
        await self.next.update_schedule(input)

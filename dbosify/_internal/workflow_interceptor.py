"""Workflow-side interceptors, mirroring the workflow portion of
``temporalio.worker`` (``temporalio/worker/_interceptor.py``).

Workflow inbound/outbound interception. The classes are re-exported from
:py:mod:`dbosify.worker` so user code
extends ``dbosify.worker.WorkflowInboundInterceptor`` /
``WorkflowOutboundInterceptor`` exactly as it would the ``temporalio.worker``
ones. A worker interceptor advertises a workflow interceptor by overriding
``Interceptor.workflow_interceptor_class`` (in ``activity_interceptor.py``,
alongside ``intercept_activity``).

The chains are built per workflow execution in ``_internal/interpreter.py``
(mirroring temporalio's chaining): inbound interceptors wrap the real
``execute_workflow`` / ``handle_signal`` / ``handle_query`` /
``handle_update_*`` dispatch, and ``init`` installs the (possibly wrapped)
outbound so ``start_activity`` / ``start_child_workflow`` / signals /
``continue_as_new`` route through it.

The ``*Input`` dataclasses are copied field-for-field from the SDK (DESIGN
§6.8) so signature parity holds; fields dbosify does not act on (e.g.
``versioning_intent``, ``initial_versioning_behavior``, ``priority``,
``disable_eager_execution``, ``arg_types``/``ret_type``) are carried but
inert. Annotations use our own types or ``Any`` (the parity test checks
parameter names/kind/default/order, not annotations).

These ``headers`` carry a real value end-to-end: the run's headers reach
``ExecuteWorkflowInput``; an inbound message's headers reach the matching
``Handle*Input``; and headers set on an outbound ``*Input`` propagate to the
activity attempt / child run / signalled workflow (header-based context
propagation — tracing, baggage). Nexus interception is
unsupported (corollary of no-server) and intentionally absent.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    List,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
)

if TYPE_CHECKING:
    from ..workflow import ActivityHandle, ChildWorkflowHandle, Info

__all__ = [
    "WorkflowInterceptorClassInput",
    "ExecuteWorkflowInput",
    "HandleSignalInput",
    "HandleQueryInput",
    "HandleUpdateInput",
    "ContinueAsNewInput",
    "SignalChildWorkflowInput",
    "SignalExternalWorkflowInput",
    "StartActivityInput",
    "StartChildWorkflowInput",
    "StartLocalActivityInput",
    "WorkflowInboundInterceptor",
    "WorkflowOutboundInterceptor",
]


# --------------------------------------------------------------------------
# workflow_interceptor_class input
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkflowInterceptorClassInput:
    """Input for :py:meth:`dbosify.worker.Interceptor.workflow_interceptor_class`.

    ``unsafe_extern_functions`` is carried for parity; dbosify has no
    workflow sandbox (DEVIATIONS dbos-native-management), so there is nothing to expose extern
    functions *into* — the mapping is inert.
    """

    unsafe_extern_functions: MutableMapping[str, Callable[..., Any]]


# --------------------------------------------------------------------------
# Inbound inputs
# --------------------------------------------------------------------------


@dataclass
class ExecuteWorkflowInput:
    """Input for :py:meth:`WorkflowInboundInterceptor.execute_workflow`."""

    type: type
    # Note, this is an unbound method
    run_fn: Callable[..., Awaitable[Any]]
    args: Sequence[Any]
    headers: Mapping[str, Any]


@dataclass
class HandleSignalInput:
    """Input for :py:meth:`WorkflowInboundInterceptor.handle_signal`."""

    signal: str
    args: Sequence[Any]
    headers: Mapping[str, Any]


@dataclass
class HandleQueryInput:
    """Input for :py:meth:`WorkflowInboundInterceptor.handle_query`."""

    id: str
    query: str
    args: Sequence[Any]
    headers: Mapping[str, Any]


@dataclass
class HandleUpdateInput:
    """Input for :py:meth:`WorkflowInboundInterceptor.handle_update_validator`
    and :py:meth:`WorkflowInboundInterceptor.handle_update_handler`.
    """

    id: str
    update: str
    args: Sequence[Any]
    headers: Mapping[str, Any]


# --------------------------------------------------------------------------
# Outbound inputs
# --------------------------------------------------------------------------


@dataclass
class ContinueAsNewInput:
    """Input for :py:meth:`WorkflowOutboundInterceptor.continue_as_new`."""

    workflow: Optional[str]
    args: Sequence[Any]
    task_queue: Optional[str]
    run_timeout: Optional[timedelta]
    task_timeout: Optional[timedelta]
    retry_policy: Optional[Any]
    memo: Optional[Mapping[str, Any]]
    search_attributes: Optional[Any]
    headers: Mapping[str, Any]
    versioning_intent: Optional[Any]
    initial_versioning_behavior: Optional[Any]
    # The types may be absent
    arg_types: Optional[List[type]]


@dataclass
class SignalChildWorkflowInput:
    """Input for :py:meth:`WorkflowOutboundInterceptor.signal_child_workflow`."""

    signal: str
    args: Sequence[Any]
    child_workflow_id: str
    headers: Mapping[str, Any]


@dataclass
class SignalExternalWorkflowInput:
    """Input for :py:meth:`WorkflowOutboundInterceptor.signal_external_workflow`."""

    signal: str
    args: Sequence[Any]
    namespace: str
    workflow_id: str
    workflow_run_id: Optional[str]
    headers: Mapping[str, Any]


@dataclass
class StartActivityInput:
    """Input for :py:meth:`WorkflowOutboundInterceptor.start_activity`."""

    activity: str
    args: Sequence[Any]
    activity_id: Optional[str]
    task_queue: Optional[str]
    schedule_to_close_timeout: Optional[timedelta]
    schedule_to_start_timeout: Optional[timedelta]
    start_to_close_timeout: Optional[timedelta]
    heartbeat_timeout: Optional[timedelta]
    retry_policy: Optional[Any]
    cancellation_type: Any
    headers: Mapping[str, Any]
    disable_eager_execution: bool
    versioning_intent: Optional[Any]
    summary: Optional[str]
    priority: Any
    # The types may be absent
    arg_types: Optional[List[type]]
    ret_type: Optional[type]


@dataclass
class StartChildWorkflowInput:
    """Input for :py:meth:`WorkflowOutboundInterceptor.start_child_workflow`."""

    workflow: str
    args: Sequence[Any]
    id: str
    task_queue: Optional[str]
    cancellation_type: Any
    parent_close_policy: Any
    execution_timeout: Optional[timedelta]
    run_timeout: Optional[timedelta]
    task_timeout: Optional[timedelta]
    id_reuse_policy: Any
    retry_policy: Optional[Any]
    cron_schedule: str
    memo: Optional[Mapping[str, Any]]
    search_attributes: Optional[Any]
    headers: Mapping[str, Any]
    versioning_intent: Optional[Any]
    static_summary: Optional[str]
    static_details: Optional[str]
    priority: Any
    # The types may be absent
    arg_types: Optional[List[type]]
    ret_type: Optional[type]


@dataclass
class StartLocalActivityInput:
    """Input for :py:meth:`WorkflowOutboundInterceptor.start_local_activity`."""

    activity: str
    args: Sequence[Any]
    activity_id: Optional[str]
    schedule_to_close_timeout: Optional[timedelta]
    schedule_to_start_timeout: Optional[timedelta]
    start_to_close_timeout: Optional[timedelta]
    retry_policy: Optional[Any]
    local_retry_threshold: Optional[timedelta]
    cancellation_type: Any
    headers: Mapping[str, Any]
    summary: Optional[str]
    # The types may be absent
    arg_types: Optional[List[type]]
    ret_type: Optional[type]


# --------------------------------------------------------------------------
# Interceptor base classes
# --------------------------------------------------------------------------


class WorkflowInboundInterceptor:
    """Inbound interceptor to wrap outbound creation, workflow execution, and
    signal/query/update handling.

    This should be extended by any workflow inbound interceptors. Advertise it
    from a worker ``Interceptor`` via ``workflow_interceptor_class``.
    """

    def __init__(self, next: WorkflowInboundInterceptor) -> None:
        """Create the inbound interceptor.

        Args:
            next: The next interceptor in the chain. The default implementation
                of all calls is to delegate to the next interceptor.
        """
        self.next = next

    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        """Initialize with an outbound interceptor.

        To add a custom outbound interceptor, wrap the given interceptor before
        sending to the next ``init`` call.
        """
        self.next.init(outbound)

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        """Called to run the workflow."""
        return await self.next.execute_workflow(input)

    async def handle_signal(self, input: HandleSignalInput) -> None:
        """Called to handle a signal."""
        return await self.next.handle_signal(input)

    async def handle_query(self, input: HandleQueryInput) -> Any:
        """Called to handle a query.

        Queries are synchronous in dbosify: the chain is
        driven to completion without suspension, so an override must not
        ``await`` anything that would park the event loop.
        """
        return await self.next.handle_query(input)

    def handle_update_validator(self, input: HandleUpdateInput) -> None:
        """Called to handle an update's validation stage (synchronous)."""
        self.next.handle_update_validator(input)

    async def handle_update_handler(self, input: HandleUpdateInput) -> Any:
        """Called to handle an update's handler."""
        return await self.next.handle_update_handler(input)


class WorkflowOutboundInterceptor:
    """Outbound interceptor to wrap calls made from within workflows.

    This should be extended by any workflow outbound interceptors.
    """

    def __init__(self, next: WorkflowOutboundInterceptor) -> None:
        """Create the outbound interceptor.

        Args:
            next: The next interceptor in the chain. The default implementation
                of all calls is to delegate to the next interceptor.
        """
        self.next = next

    def continue_as_new(self, input: ContinueAsNewInput) -> NoReturn:
        """Called for every :py:func:`dbosify.workflow.continue_as_new` call."""
        self.next.continue_as_new(input)

    def info(self) -> "Info":
        """Called for every :py:func:`dbosify.workflow.info` call."""
        return self.next.info()

    async def signal_child_workflow(self, input: SignalChildWorkflowInput) -> None:
        """Called for every
        :py:meth:`dbosify.workflow.ChildWorkflowHandle.signal` call.
        """
        return await self.next.signal_child_workflow(input)

    async def signal_external_workflow(
        self, input: SignalExternalWorkflowInput
    ) -> None:
        """Called for every
        :py:meth:`dbosify.workflow.ExternalWorkflowHandle.signal` call.
        """
        return await self.next.signal_external_workflow(input)

    def start_activity(self, input: StartActivityInput) -> "ActivityHandle":
        """Called for every :py:func:`dbosify.workflow.start_activity` and
        :py:func:`dbosify.workflow.execute_activity` call.
        """
        return self.next.start_activity(input)

    async def start_child_workflow(
        self, input: StartChildWorkflowInput
    ) -> "ChildWorkflowHandle":
        """Called for every :py:func:`dbosify.workflow.start_child_workflow`
        and :py:func:`dbosify.workflow.execute_child_workflow` call.
        """
        return await self.next.start_child_workflow(input)

    def start_local_activity(self, input: StartLocalActivityInput) -> "ActivityHandle":
        """Called for every :py:func:`dbosify.workflow.start_local_activity`
        and :py:func:`dbosify.workflow.execute_local_activity` call.
        """
        return self.next.start_local_activity(input)

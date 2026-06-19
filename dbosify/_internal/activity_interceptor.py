"""Worker-side interceptors, mirroring the activity portion of
``temporalio.worker`` (``temporalio/worker/_interceptor.py``).

Activity inbound/outbound interception lives here; the workflow
inbound/outbound classes and their ``*Input`` dataclasses live in
:py:mod:`._internal.workflow_interceptor`. Both sets are re-exported from
:py:mod:`dbosify.worker` so user code extends
``dbosify.worker.Interceptor`` exactly as it would
``temporalio.worker.Interceptor``.

A worker ``Interceptor`` advertises an activity interceptor via
``intercept_activity`` and a workflow interceptor via
``workflow_interceptor_class``. Nexus interception is unsupported
(DEVIATIONS D1) and intentionally absent.

The activity chain is built per attempt in ``_internal/activities.py``
(mirroring temporalio's ``_activity.py`` chaining): inbound interceptors wrap
the real sync/async dispatch; outbound interceptors wrap ``activity.info()`` /
``activity.heartbeat()``. The workflow chains are built per execution in
``_internal/interpreter.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Mapping, Optional, Sequence, Type

if TYPE_CHECKING:
    from ..activity import Info
    from .workflow_interceptor import (
        WorkflowInboundInterceptor,
        WorkflowInterceptorClassInput,
    )

__all__ = [
    "Interceptor",
    "ActivityInboundInterceptor",
    "ActivityOutboundInterceptor",
    "ExecuteActivityInput",
]


class Interceptor:
    """Interceptor for workers.

    This should be extended by any worker interceptors. Pass instances to
    ``dbosify.worker.Worker(interceptors=[...])``.
    """

    def intercept_activity(
        self, next: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        """Method called for intercepting an activity.

        Args:
            next: The underlying inbound interceptor this interceptor should
                delegate to.

        Returns:
            The new interceptor that will be used for the activity.
        """
        return next

    def workflow_interceptor_class(
        self, input: "WorkflowInterceptorClassInput"
    ) -> "Optional[Type[WorkflowInboundInterceptor]]":
        """Class that will be instantiated and used to intercept workflows.

        Called once per workflow execution (DESIGN §6.8). The returned class
        must take the same constructor as
        :py:meth:`WorkflowInboundInterceptor.__init__` (a single ``next``).
        Returning ``None`` (the default) means this interceptor does not
        intercept workflows.

        Args:
            input: Carries ``unsafe_extern_functions`` for parity; inert here
                (dbosify has no workflow sandbox, DEVIATIONS D3).

        Returns:
            The class to construct to intercept each workflow, or ``None``.
        """
        return None


@dataclass
class ExecuteActivityInput:
    """Input for :py:meth:`ActivityInboundInterceptor.execute_activity`."""

    fn: Callable[..., Any]
    args: Sequence[Any]
    # Always ``None`` in dbosify: sync activities run via
    # ``asyncio.to_thread``, not a user-supplied executor. Present for parity.
    executor: Any | None
    # Headers (str -> Payload) a workflow outbound interceptor set on
    # StartActivityInput, propagated here (DEVIATIONS D24); empty when nothing
    # set them. Decode values with ``activity.payload_converter()``.
    headers: Mapping[str, Any]


class ActivityInboundInterceptor:
    """Inbound interceptor to wrap outbound creation and activity execution.

    This should be extended by any activity inbound interceptors.
    """

    def __init__(self, next: ActivityInboundInterceptor) -> None:
        """Create the inbound interceptor.

        Args:
            next: The next interceptor in the chain. The default implementation
                of all calls is to delegate to the next interceptor.
        """
        self.next = next

    def init(self, outbound: ActivityOutboundInterceptor) -> None:
        """Initialize with an outbound interceptor.

        To add a custom outbound interceptor, wrap the given interceptor before
        sending to the next ``init`` call.
        """
        self.next.init(outbound)

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        """Called to invoke the activity."""
        return await self.next.execute_activity(input)


class ActivityOutboundInterceptor:
    """Outbound interceptor to wrap calls made from within activities.

    This should be extended by any activity outbound interceptors.
    """

    def __init__(self, next: ActivityOutboundInterceptor) -> None:
        """Create the outbound interceptor.

        Args:
            next: The next interceptor in the chain. The default implementation
                of all calls is to delegate to the next interceptor.
        """
        self.next = next

    def info(self) -> Info:
        """Called for every :py:func:`dbosify.activity.info` call."""
        return self.next.info()

    def heartbeat(self, *details: Any) -> None:
        """Called for every :py:func:`dbosify.activity.heartbeat` call."""
        self.next.heartbeat(*details)

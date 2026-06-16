"""Worker-side interceptors, mirroring the activity portion of
``temporalio.worker`` (``temporalio/worker/_interceptor.py``).

This is the Phase-3 surface: activity inbound/outbound interception. The
classes are re-exported from :py:mod:`temporal_dbos.worker` so user code
extends ``temporal_dbos.worker.Interceptor`` exactly as it would
``temporalio.worker.Interceptor``.

Workflow inbound/outbound interception (``workflow_interceptor_class`` and the
``WorkflowInbound/Outbound`` classes) is Phase 4 and intentionally absent here;
Nexus interception is unsupported (DEVIATIONS D1). The base ``Interceptor``
therefore exposes only ``intercept_activity``.

The chain is built per activity attempt in
``_internal/activities.py`` (mirroring temporalio's
``_activity.py`` chaining): inbound interceptors wrap the real
sync/async dispatch; outbound interceptors wrap ``activity.info()`` /
``activity.heartbeat()``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Mapping, Sequence

if TYPE_CHECKING:
    from ..activity import Info

__all__ = [
    "Interceptor",
    "ActivityInboundInterceptor",
    "ActivityOutboundInterceptor",
    "ExecuteActivityInput",
]


class Interceptor:
    """Interceptor for workers.

    This should be extended by any worker interceptors. Pass instances to
    ``temporal_dbos.worker.Worker(interceptors=[...])``.
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


@dataclass
class ExecuteActivityInput:
    """Input for :py:meth:`ActivityInboundInterceptor.execute_activity`."""

    fn: Callable[..., Any]
    args: Sequence[Any]
    # Always ``None`` in temporal_dbos: sync activities run via
    # ``asyncio.to_thread``, not a user-supplied executor. Present for parity.
    executor: Any | None
    # Always empty in temporal_dbos: there is no header-propagation path until
    # workflow interceptors (Phase 4). Present for parity.
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
        """Called for every :py:func:`temporal_dbos.activity.info` call."""
        return self.next.info()

    def heartbeat(self, *details: Any) -> None:
        """Called for every :py:func:`temporal_dbos.activity.heartbeat` call."""
        self.next.heartbeat(*details)

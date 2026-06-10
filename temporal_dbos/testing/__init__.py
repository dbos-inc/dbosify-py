"""Test framework module, mirroring ``temporalio.testing``.

Phase 1 ships :py:class:`ActivityEnvironment` (pure in-memory, no database).
``WorkflowEnvironment.start_local`` arrives in Phase 2 and time-skipping in
Phase 4 (see DESIGN §6.10).
"""

import asyncio
from typing import Any, Callable, Coroutine, TypeVar, Union, overload

from .. import activity as _activity

__all__ = ["ActivityEnvironment"]

_R = TypeVar("_R")

_default_info = _activity.Info(
    activity_id="test",
    activity_type="unknown",
    attempt=1,
    task_queue="test",
    workflow_id="test",
    workflow_run_id="test-run",
    workflow_type="test",
    is_local=False,
)


class ActivityEnvironment:
    """Activity environment for testing activity code directly: runs the
    function with the activity context (``activity.info()``, ``heartbeat``,
    cancellation observation) set, entirely in memory.

    Attributes:
        info: The info handed to activities; replace to customize.
        on_heartbeat: Called with the details of each ``activity.heartbeat``.
    """

    def __init__(self) -> None:
        self.info: _activity.Info = _default_info
        self.on_heartbeat: Callable[..., None] = lambda *args: None
        self._context = _activity._Context(
            info=self.info, on_heartbeat=lambda *details: self.on_heartbeat(*details)
        )

    def cancel(self) -> None:
        """Mark the environment's activity as cancelled: ``is_cancelled()``
        becomes true and ``wait_for_cancelled*`` unblocks.
        """
        self._context.cancelled.set()

    @overload
    def run(
        self,
        fn: Callable[..., Coroutine[Any, Any, _R]],
        *args: Any,
        **kwargs: Any,
    ) -> Coroutine[Any, Any, _R]: ...

    @overload
    def run(self, fn: Callable[..., _R], *args: Any, **kwargs: Any) -> _R: ...

    def run(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Run the given activity callable in this environment. Returns the
        result, or a coroutine to await if the activity is async.
        """
        self._context.info = self.info

        if asyncio.iscoroutinefunction(fn):

            async def run_async() -> Any:
                token = _activity._current_context.set(self._context)
                try:
                    return await fn(*args, **kwargs)
                finally:
                    _activity._current_context.reset(token)

            return run_async()

        token = _activity._current_context.set(self._context)
        try:
            return fn(*args, **kwargs)
        finally:
            _activity._current_context.reset(token)

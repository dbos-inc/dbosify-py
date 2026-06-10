"""Worker for running workflows and activities, mirroring
``temporalio.worker`` in shape while taking DBOS machinery directly: a
Worker is constructed from a ``dbos.DBOSConfig`` and owns the process's DBOS
lifecycle outright.

Exactly **one Worker per process** is supported for now: DBOS's launchable
runtime (queue listeners, notification listener, recovery) is process-global,
so multiple in-process Workers would share lifecycle and registrations in
ways that diverge from Temporal's worker-isolation model (see README
deviations). One worker per process is also the dominant production layout.

``await worker.run()`` launches DBOS — recovering this executor's pending
workflows, mirroring Temporal worker restart semantics — and blocks until
``shutdown()``. ``async with`` is supported and is what tests use constantly.

Parameter mapping: ``max_concurrent_workflow_tasks`` -> task queue
``worker_concurrency``; ``graceful_shutdown_timeout`` ->
``DBOS.destroy(workflow_completion_timeout_sec)``. Tuner/poller/sandbox
arguments are accepted and ignored with a debug log.
"""

import asyncio
import logging
from datetime import timedelta
from typing import Any, Callable, Optional, Sequence, Type

from dbos import DBOS, DBOSConfig

from ._internal import dispatcher as _dispatcher

__all__ = ["Worker"]

logger = logging.getLogger("temporal_dbos.worker")

# The one live Worker in this process (see module docstring).
_live_worker: Optional["Worker"] = None


def _reset_for_tests() -> None:
    """Clear the per-process worker slot and all temporal-dbos/DBOS state."""
    global _live_worker
    _live_worker = None
    DBOS.destroy(destroy_registry=True)
    _dispatcher._reset_for_tests()


class Worker:
    """A worker to process workflows and/or activities, built on a DBOS
    runtime configured by ``config``.
    """

    def __init__(
        self,
        config: DBOSConfig,
        *,
        task_queue: str,
        workflows: Sequence[Type[Any]] = [],
        activities: Sequence[Callable[..., Any]] = [],
        activity_executor: Optional[Any] = None,
        workflow_task_executor: Optional[Any] = None,
        max_concurrent_workflow_tasks: Optional[int] = None,
        max_concurrent_activities: Optional[int] = None,
        graceful_shutdown_timeout: timedelta = timedelta(),
        workflow_failure_exception_types: Sequence[Type[BaseException]] = [],
        **unsupported: Any,
    ) -> None:
        """Create the process's worker. Registration (workflow types,
        activity types, the task queue) happens at construction; execution
        and recovery start at :py:meth:`run`.
        """
        global _live_worker
        if _live_worker is not None:
            raise RuntimeError(
                "Only one Worker per process is supported (an existing "
                "Worker owns this process's DBOS runtime)"
            )
        if not workflows and not activities:
            raise ValueError("At least one workflow and/or activity must be specified")
        for key, value in {
            "activity_executor": activity_executor,
            "workflow_task_executor": workflow_task_executor,
            "max_concurrent_activities": max_concurrent_activities,
            **unsupported,
        }.items():
            if value is not None:
                logger.debug("Worker: ignoring unsupported option %r", key)

        self._task_queue = task_queue
        self._max_concurrent_workflow_tasks = max_concurrent_workflow_tasks
        self._graceful_shutdown_timeout = graceful_shutdown_timeout
        DBOS(config=config)
        _dispatcher.register_worker(
            workflows=workflows,
            activities=activities,
            failure_exception_types=workflow_failure_exception_types,
        )
        # The task queue is a database-backed DBOS queue; this process
        # dequeues only from its declared listen set (plus DBOS's internal
        # queue). The queue itself is registered in run(), after launch,
        # since persisting its config needs the system database.
        DBOS.listen_queues([task_queue])
        self._shutdown_event: Optional["asyncio.Event"] = None
        self._run_task: Optional["asyncio.Task[None]"] = None
        self._finished = False
        _live_worker = self

    @property
    def task_queue(self) -> str:
        """Task queue this worker is on."""
        return self._task_queue

    def is_running(self) -> bool:
        """Whether the worker is running (between run() and shutdown())."""
        return self._shutdown_event is not None

    async def run(self) -> None:
        """Launch DBOS (recovering pending workflows for this executor) and
        block until :py:meth:`shutdown` is called. One run per worker.
        """
        global _live_worker
        if self._shutdown_event is not None:
            raise RuntimeError("Already running")
        if self._finished:
            raise RuntimeError("Worker already shut down; create a new one")
        self._shutdown_event = asyncio.Event()
        DBOS.launch()
        try:
            # Persist this worker's queue configuration. The default
            # conflict policy (update_if_latest_version) keeps an older
            # worker in a rolling deploy from clobbering newer queue config.
            await DBOS.register_queue_async(
                self._task_queue,
                worker_concurrency=self._max_concurrent_workflow_tasks,
            )
            await self._shutdown_event.wait()
        finally:
            self._shutdown_event = None
            self._finished = True
            DBOS.destroy(
                workflow_completion_timeout_sec=int(
                    self._graceful_shutdown_timeout.total_seconds()
                )
            )
            _live_worker = None

    async def shutdown(self) -> None:
        """Initiate shutdown and wait for :py:meth:`run` to return."""
        if self._shutdown_event is not None:
            self._shutdown_event.set()
        if self._run_task is not None:
            await self._run_task
            self._run_task = None

    async def __aenter__(self) -> "Worker":
        """Start the worker in the background."""
        self._run_task = asyncio.create_task(self.run())
        # Yield once so run() reaches its launch before user code proceeds.
        await asyncio.sleep(0)
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        """Shut the worker down."""
        await self.shutdown()

"""Worker for running workflows and activities, mirroring
``temporalio.worker``.

A Worker upgrades the process's shared runtime to full DBOS mode: it
registers the per-type dispatchers and activity steps, and registers its
task queue as a DBOS queue (DESIGN §3, §5). ``await worker.run()`` launches
DBOS — recovering this executor's pending workflows, mirroring Temporal
worker restart semantics — and blocks until ``shutdown()``. ``async with``
is supported and is what tests use constantly.

Parameter mapping (DESIGN §5): ``max_concurrent_workflow_tasks`` -> queue
``worker_concurrency``; ``build_id`` -> DBOS ``application_version``;
``graceful_shutdown_timeout`` -> ``DBOS.destroy(workflow_completion_timeout_sec)``.
Tuner/poller/sandbox arguments are accepted and ignored with a debug log.
"""

import asyncio
import logging
from datetime import timedelta
from typing import Any, Callable, Optional, Sequence, Type

from ._internal import dispatcher as _dispatcher
from ._internal import runtime as _runtime
from .client import Client

__all__ = ["Worker"]

logger = logging.getLogger("temporal_dbos.worker")


class Worker:
    """A worker to process workflows and/or activities."""

    def __init__(
        self,
        client: Client,
        *,
        task_queue: str,
        workflows: Sequence[Type[Any]] = [],
        activities: Sequence[Callable[..., Any]] = [],
        activity_executor: Optional[Any] = None,
        workflow_task_executor: Optional[Any] = None,
        max_concurrent_workflow_tasks: Optional[int] = None,
        max_concurrent_activities: Optional[int] = None,
        build_id: Optional[str] = None,
        graceful_shutdown_timeout: timedelta = timedelta(),
        workflow_failure_exception_types: Sequence[Type[BaseException]] = [],
        **unsupported: Any,
    ) -> None:
        """Create a worker for the given task queue. Registration happens at
        construction; execution (and recovery) starts at :py:meth:`run`.
        """
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
        self._graceful_shutdown_timeout = graceful_shutdown_timeout
        self._runtime = _runtime.get_runtime()
        self._runtime.ensure_full_runtime(app_version=build_id)
        _dispatcher.register_worker(
            workflows=workflows,
            activities=activities,
            failure_exception_types=workflow_failure_exception_types,
        )
        self._runtime.register_queue(
            task_queue, worker_concurrency=max_concurrent_workflow_tasks
        )
        self._shutdown_event: Optional["asyncio.Event"] = None
        self._run_task: Optional["asyncio.Task[None]"] = None

    @property
    def task_queue(self) -> str:
        """Task queue this worker is on."""
        return self._task_queue

    def is_running(self) -> bool:
        """Whether the worker is running (between run() and shutdown())."""
        return self._shutdown_event is not None

    async def run(self) -> None:
        """Launch the runtime (recovering pending workflows for this
        executor) and block until :py:meth:`shutdown` is called.
        """
        if self._shutdown_event is not None:
            raise RuntimeError("Already running")
        self._shutdown_event = asyncio.Event()
        self._runtime.launch()
        try:
            await self._shutdown_event.wait()
        finally:
            self._shutdown_event = None
            self._runtime.release(
                workflow_completion_timeout_sec=int(
                    self._graceful_shutdown_timeout.total_seconds()
                )
            )

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

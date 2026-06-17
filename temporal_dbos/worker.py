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
import concurrent.futures
import logging
from datetime import timedelta
from typing import Any, Callable, Optional, Sequence, Type

from dbos import DBOS, DBOSConfig

from ._internal import conversion
from ._internal import dispatcher as _dispatcher
from ._internal.activity_interceptor import (
    ActivityInboundInterceptor,
    ActivityOutboundInterceptor,
    ExecuteActivityInput,
    Interceptor,
)
from ._internal.serializer import TEMPORAL_SERIALIZER
from ._internal.workflow_interceptor import (
    ContinueAsNewInput,
    ExecuteWorkflowInput,
    HandleQueryInput,
    HandleSignalInput,
    HandleUpdateInput,
    SignalChildWorkflowInput,
    SignalExternalWorkflowInput,
    StartActivityInput,
    StartChildWorkflowInput,
    StartLocalActivityInput,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
)
from .converter import DataConverter

__all__ = [
    "ActivityInboundInterceptor",
    "ActivityOutboundInterceptor",
    "ContinueAsNewInput",
    "ExecuteActivityInput",
    "ExecuteWorkflowInput",
    "HandleQueryInput",
    "HandleSignalInput",
    "HandleUpdateInput",
    "Interceptor",
    "SignalChildWorkflowInput",
    "SignalExternalWorkflowInput",
    "StartActivityInput",
    "StartChildWorkflowInput",
    "StartLocalActivityInput",
    "WorkflowInboundInterceptor",
    "WorkflowInterceptorClassInput",
    "WorkflowOutboundInterceptor",
    "Worker",
]

logger = logging.getLogger("temporal_dbos.worker")

# A stable default DBOS application version. DBOS scopes workflow recovery and
# queue dequeuing by ``application_version`` and otherwise auto-computes it from
# a hash of the registered code — so any code change would change the version
# and strand every in-flight workflow under the old one (a new-code worker never
# recovers or re-dequeues it), cooperating workers that register different
# function sets (e.g. a workflow worker and an activity-only worker) would never
# share a version, and ``workflow.patched()`` — whose whole purpose is to let
# redeployed code keep serving pre-patch runs — would never reach those runs.
# Pinning a constant makes all workers agree by default and deploys preserve
# in-flight work; a genuinely incompatible change then surfaces as a replay
# ``NondeterminismError`` (the same contract as Temporal, managed with
# ``workflow.patched()``). See DESIGN §6.8 / DEVIATIONS D27. Distinct apps
# sharing one database should set ``application_version`` explicitly to keep
# their versions apart.
DEFAULT_APP_VERSION = "0.1"

# The one live Worker in this process (see module docstring).
_live_worker: Optional["Worker"] = None


def _with_default_app_version(config: DBOSConfig) -> DBOSConfig:
    """Pin :data:`DEFAULT_APP_VERSION` unless the caller set
    ``application_version`` in the ``DBOSConfig``. Pass it as ``None`` to opt
    into DBOS's code-hash auto-versioning instead.
    """
    if "application_version" in config:
        return config
    return {**config, "application_version": DEFAULT_APP_VERSION}


def _reset_for_tests() -> None:
    """Clear the per-process worker slot and all temporal-dbos/DBOS state."""
    global _live_worker
    _live_worker = None
    DBOS.destroy(destroy_registry=True)
    _dispatcher._reset_for_tests()
    conversion.reset_converter()


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
        data_converter: DataConverter = DataConverter.default,
        interceptors: Sequence[Interceptor] = [],
        **unsupported: Any,
    ) -> None:
        """Create the process's worker. Registration (workflow types,
        activity types, the task queue) happens at construction; execution
        and recovery start at :py:meth:`run`.
        """
        global _live_worker
        if not task_queue or not isinstance(task_queue, str):
            raise ValueError("task_queue must be a non-empty string")
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
        # The interpreter (in this process) decodes run args / encodes results
        # with this converter; configure the Client the same.
        conversion.set_converter(data_converter)
        # JSON transport (replaces DBOS's default pickle). All processes on the
        # database must share this serializer's name (see serializer.py).
        config = {**config, "serializer": TEMPORAL_SERIALIZER}
        # Pin a stable app version so redeploys don't strand in-flight workflows
        # and workflow.patched() actually reaches pre-patch runs (DEFAULT_APP_VERSION).
        config = _with_default_app_version(config)
        DBOS(config=config)
        _dispatcher.register_worker(
            workflows=workflows,
            activities=activities,
            failure_exception_types=workflow_failure_exception_types,
            interceptors=interceptors,
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
        # DBOS launched with a running loop adopts it: workflow coroutines
        # and our own DBOS async calls all run here, and every DBOS async
        # API installs DBOS's thread pool as this loop's *default executor*
        # — which destroy() shuts down without restoring. Capture the
        # original so the loop's asyncio.to_thread still works after the
        # worker exits.
        loop = asyncio.get_running_loop()
        original_executor = getattr(loop, "_default_executor", None)
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
            # TODO(dbos-destroy-deadlock): remove the dedicated-thread dance
            # below (run destroy() inline) once DBOS.destroy no longer
            # self-deadlocks when called from its adopted main loop. Not yet
            # filed upstream — needs a minimal repro first; track here until
            # there is an issue/PR number to reference.
            # destroy() must not run ON the loop DBOS adopted: when any
            # workflow-timeout task is still pending (every run_timeout
            # workflow parks one until its deadline), destroy submits a
            # cancellation coroutine to the main loop and blocks on its
            # result — a self-deadlock if called from that loop. Run it on
            # a dedicated thread so the loop stays free to execute the
            # cancellation. A fresh single-use thread, not asyncio.to_thread:
            # the loop's default executor is DBOS's own pool, which destroy
            # shuts down.
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="tdb-worker-shutdown"
            ) as shutdown_pool:
                await loop.run_in_executor(
                    shutdown_pool,
                    lambda: DBOS.destroy(
                        workflow_completion_timeout_sec=int(
                            self._graceful_shutdown_timeout.total_seconds()
                        )
                    ),
                )
            if original_executor is None or getattr(
                original_executor, "_shutdown", False
            ):
                # Same construction asyncio uses for its lazy default.
                original_executor = concurrent.futures.ThreadPoolExecutor(
                    thread_name_prefix="asyncio"
                )
            loop.set_default_executor(original_executor)
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

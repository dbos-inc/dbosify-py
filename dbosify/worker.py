"""Worker for running workflows and activities, mirroring
``temporalio.worker`` in shape while taking DBOS machinery directly: a
Worker is constructed from a ``dbos.DBOSConfig`` and owns the process's DBOS
lifecycle outright.

Exactly **one Worker per process** is supported: DBOS's launchable
runtime (queue listeners, notification listener, recovery) is process-global,
so multiple in-process Workers would share lifecycle and registrations in
ways that diverge from Temporal's worker-isolation model. One worker per
process is also the dominant production layout.

``await worker.run()`` launches DBOS — recovering this executor's pending
workflows, mirroring Temporal worker restart semantics — and blocks until
``shutdown()``. ``async with`` is supported and is what tests use constantly.

Parameter mapping onto DBOS: ``max_concurrent_workflow_tasks``
-> the workflow task queue's ``worker_concurrency``; ``max_concurrent_activities``
/ ``max_concurrent_local_activities`` -> a per-process semaphore around activity
execution (and the activity queue's ``worker_concurrency`` for an activities-only
worker); ``max_activities_per_second`` / ``max_task_queue_activities_per_second``
-> the activity queue's rate ``limiter``; ``activity_executor`` -> the executor
sync activities run on; ``identity`` -> the DBOS ``executor_id``;
``graceful_shutdown_timeout`` -> ``DBOS.destroy(workflow_completion_timeout_sec)``;
``on_fatal_error`` -> called if the run loop raises. Every other temporalio
Worker option is classified (and the classification is machine-checked by
``tests/unit/test_worker_param_audit.py``): poller/sandbox/sticky-cache/heartbeat
options have no DBOS analog and are accepted-and-ignored (inert) with a debug
log, while behavior-changing options we can't fulfil — ``tuner``, ``plugins``,
``nexus_service_handlers`` — are **rejected** (raise) rather than silently
no-op'd.
"""

import asyncio
import concurrent.futures
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Awaitable, Callable, Optional, Sequence, Type

from dbos import DBOS, DBOSConfig
from dbos._queue import QueueRateLimit

from . import activity as _activity
from ._internal import conversion
from ._internal import dispatcher as _dispatcher
from ._internal import registry as _registry
from ._internal.activity_interceptor import (
    ActivityInboundInterceptor,
    ActivityOutboundInterceptor,
    ExecuteActivityInput,
    Interceptor,
)
from ._internal.namespaces import DEFAULT_NAMESPACE, namespace_schema
from ._internal.replay import (
    Replayer,
    WorkflowReplayResult,
    WorkflowReplayResults,
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
from .common import VersioningBehavior, WorkerDeploymentVersion
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
    "Replayer",
    "SignalChildWorkflowInput",
    "SignalExternalWorkflowInput",
    "StartActivityInput",
    "StartChildWorkflowInput",
    "StartLocalActivityInput",
    "WorkerDeploymentConfig",
    "WorkflowInboundInterceptor",
    "WorkflowInterceptorClassInput",
    "WorkflowOutboundInterceptor",
    "WorkflowReplayResult",
    "WorkflowReplayResults",
    "Worker",
]

logger = logging.getLogger("dbosify.worker")

# A stable default DBOS application version. Pinning a constant (vs DBOS's code-hash
# auto-version) lets workers agree and deploys preserve in-flight work (DESIGN §6.8).
DEFAULT_APP_VERSION = "0.1"

# Behavior-changing AND unsupported options: passing a non-default value raises
# rather than silently no-ops. arg name -> hint; arrive via ``**unsupported``.
_REJECTED_OPTIONS = {
    "nexus_service_handlers": "Nexus is not supported (DESIGN §1)",
    "tuner": "resource-based slot tuning has no DBOS analog; use "
    "max_concurrent_workflow_tasks / max_concurrent_activities",
    "plugins": "Worker plugins are not supported; use interceptors=",
}

# The one live Worker in this process (see module docstring).
_live_worker: Optional["Worker"] = None


def _rate_limiter(rate_per_second: Optional[float]) -> Optional[QueueRateLimit]:
    """Map an activities-per-second rate to a DBOS queue ``limiter`` (no more
    than ``limit`` starts per ``period`` seconds). None when unset."""
    if rate_per_second is None:
        return None
    if rate_per_second <= 0:
        raise ValueError("activities-per-second must be positive")
    if rate_per_second >= 1:
        # Whole starts per 1s window (an integer limit; fractional rates ≥1 round).
        return {"limit": round(rate_per_second), "period": 1.0}
    # Sub-1 rate: one start per 1/rate seconds (exact).
    return {"limit": 1, "period": 1.0 / rate_per_second}


def _with_default_app_version(config: DBOSConfig) -> DBOSConfig:
    """Pin :data:`DEFAULT_APP_VERSION` unless the caller set
    ``application_version`` in the ``DBOSConfig``. Pass it as ``None`` to opt
    into DBOS's code-hash auto-versioning instead.
    """
    if "application_version" in config:
        return config
    return {**config, "application_version": DEFAULT_APP_VERSION}


def _reset_for_tests() -> None:
    """Clear the per-process worker slot and all dbosify/DBOS state."""
    global _live_worker
    _live_worker = None
    DBOS.destroy(destroy_registry=True)
    _dispatcher._reset_for_tests()
    conversion.reset_converter()
    _registry.set_worker_deployment_name(None)
    detached_client = _activity._teardown_worker_state()
    if detached_client is not None:
        detached_client._dbos_client.destroy()


@dataclass(frozen=True)
class WorkerDeploymentConfig:
    """Options for configuring the Worker Versioning feature, mirroring
    ``temporalio.worker.WorkerDeploymentConfig``.

    The ``version.build_id`` becomes the DBOS ``application_version``, which DBOS
    uses to pin workflow recovery/dequeue — i.e. Temporal's PINNED behavior,
    enforced. ``default_versioning_behavior`` / ``use_worker_versioning`` are
    accepted for parity; AUTO_UPGRADE has no DBOS analog (DEVIATIONS worker-versioning).
    """

    version: WorkerDeploymentVersion
    use_worker_versioning: bool
    default_versioning_behavior: VersioningBehavior = VersioningBehavior.UNSPECIFIED


class Worker:
    """A worker to process workflows and/or activities, built on a DBOS
    runtime configured by ``config``.
    """

    def __init__(
        self,
        config: DBOSConfig,
        *,
        task_queue: str,
        namespace: str = DEFAULT_NAMESPACE,
        workflows: Sequence[Type[Any]] = [],
        activities: Sequence[Callable[..., Any]] = [],
        activity_executor: Optional[Any] = None,
        workflow_task_executor: Optional[Any] = None,
        max_concurrent_workflow_tasks: Optional[int] = None,
        max_concurrent_activities: Optional[int] = None,
        max_concurrent_local_activities: Optional[int] = None,
        max_activities_per_second: Optional[float] = None,
        max_task_queue_activities_per_second: Optional[float] = None,
        identity: Optional[str] = None,
        on_fatal_error: Optional[Callable[[BaseException], Awaitable[None]]] = None,
        graceful_shutdown_timeout: timedelta = timedelta(),
        workflow_failure_exception_types: Sequence[Type[BaseException]] = [],
        data_converter: DataConverter = DataConverter.default,
        interceptors: Sequence[Interceptor] = [],
        build_id: Optional[str] = None,
        use_worker_versioning: bool = False,
        deployment_config: Optional[WorkerDeploymentConfig] = None,
        **unsupported: Any,
    ) -> None:
        """Create the process's worker. Registration (workflow types,
        activity types, the task queue) happens at construction; execution
        and recovery start at :py:meth:`run`.

        ``build_id`` / ``deployment_config`` set the worker's deployment version
        (surfaced via ``workflow.Info.get_current_deployment_version()``). The
        build ID becomes the DBOS ``application_version``, which DBOS uses to
        scope workflow recovery and queue dequeue — so a workflow is recovered
        and continued only on workers of its build ID. That pinning *is*
        Temporal's PINNED versioning behavior, enforced. What DBOS has no analog
        for is AUTO_UPGRADE (moving a running workflow to a newer version) and
        the cluster routing-fleet / ramping concepts; see DEVIATIONS worker-versioning.
        ``use_worker_versioning`` is accepted for parity. When neither build_id
        nor deployment_config is given, the deployment version is derived from
        the DBOS application name + application_version.
        """
        global _live_worker
        if not task_queue or not isinstance(task_queue, str):
            raise ValueError("task_queue must be a non-empty string")
        if _live_worker is not None:
            raise RuntimeError(
                "Only one Worker per process is supported; one already exists"
            )
        if not workflows and not activities:
            raise ValueError("At least one workflow and/or activity must be specified")
        if deployment_config is not None and build_id is not None:
            raise ValueError("Cannot set both build_id and deployment_config")
        if use_worker_versioning and deployment_config is not None:
            # Mirror temporalio: use_worker_versioning is the deprecated knob
            # paired with build_id; it cannot be combined with deployment_config.
            raise ValueError(
                "use_worker_versioning cannot be combined with deployment_config"
            )
        if use_worker_versioning and build_id is None:
            # Mirror temporalio: opting into versioning with no version to pin to
            # is a silent misconfiguration (the worker would run unversioned).
            raise ValueError(
                "build_id must be specified when use_worker_versioning is True"
            )
        # Behavior-changing options we can't fulfil are *rejected*, not silently
        # ignored: a user passing these expects an effect we can't deliver.
        for key, hint in _REJECTED_OPTIONS.items():
            if unsupported.get(key):
                raise NotImplementedError(
                    f"Worker(...) {key}= is not supported: {hint}"
                )
        # Inert options (no DBOS analog) are accepted and ignored with a debug log.
        for key, value in {
            "workflow_task_executor": workflow_task_executor,
            **unsupported,
        }.items():
            if value is not None:
                logger.debug("Worker: ignoring unsupported (inert) option %r", key)

        self._on_fatal_error = on_fatal_error
        self._task_queue = task_queue
        self._namespace = namespace
        self._max_concurrent_workflow_tasks = max_concurrent_workflow_tasks
        self._graceful_shutdown_timeout = graceful_shutdown_timeout
        # Activity concurrency cap: max_concurrent_activities, else
        # max_concurrent_local_activities (both kinds run as steps, sharing one cap).
        self._activity_concurrency = (
            max_concurrent_activities
            if max_concurrent_activities is not None
            else max_concurrent_local_activities
        )
        # Whether the worker dequeues activities (vs. only workflows): its
        # worker_concurrency is then the activity cap, not the workflow-task cap.
        self._activities_only = bool(activities) and not workflows
        # Activity rate limit → DBOS queue limiter (queue-wide): the task-queue
        # knob maps exactly; the per-worker knob is a queue-wide approximation.
        self._activity_rate_per_second = (
            max_task_queue_activities_per_second
            if max_task_queue_activities_per_second is not None
            else max_activities_per_second
        )
        # The interpreter decodes run args / encodes results with this
        # converter; configure the Client the same.
        conversion.set_converter(data_converter)
        # The namespace owns the DBOS system schema (DEVIATIONS no-server); a
        # conflicting explicit dbos_system_schema is an error.
        schema = namespace_schema(namespace)
        configured_schema = config.get("dbos_system_schema")
        if configured_schema is not None and configured_schema != schema:
            raise ValueError(
                f"DBOSConfig dbos_system_schema {configured_schema!r} conflicts "
                f"with namespace {namespace!r} (schema {schema!r}); set the "
                "namespace, not dbos_system_schema"
            )
        # JSON transport (replaces DBOS's default pickle). All processes on the
        # database must share this serializer's name (see serializer.py).
        config = {
            **config,
            "serializer": TEMPORAL_SERIALIZER,
            "dbos_system_schema": schema,
        }
        # Worker identity → DBOS executor_id, which also *scopes recovery*
        # (failover): a custom identity should be stable per fleet, not per process.
        if identity is not None:
            config = {**config, "executor_id": identity}
        # An explicit build_id / deployment_config IS the DBOS application_version
        # (DEVIATIONS worker-versioning); without one, pin DEFAULT_APP_VERSION.
        explicit_build = (
            deployment_config.version.build_id
            if deployment_config is not None
            else build_id
        )
        if explicit_build is not None:
            if not explicit_build:
                raise ValueError("build_id must be a non-empty string")
            # A build id IS the application_version, so an explicitly-set
            # application_version is contradictory (check key presence to catch ``None``).
            if (
                "application_version" in config
                and config["application_version"] != explicit_build
            ):
                raise ValueError(
                    f"build id {explicit_build!r} conflicts with the DBOSConfig "
                    f"application_version {config['application_version']!r}; set only one"
                )
            config = {**config, "application_version": explicit_build}
        config = _with_default_app_version(config)
        DBOS(config=config)
        # Deployment name surfaced via get_current_deployment_version(): the
        # explicit deployment_config name, else the DBOS app name.
        _registry.set_worker_deployment_name(
            deployment_config.version.deployment_name
            if deployment_config is not None
            else config.get("name", "")
        )
        # Arm activity worker-lifecycle state (is_worker_shutdown(), client())
        # for this run, with the concurrency cap and sync-activity executor.
        _activity._on_worker_start(
            config,
            activity_concurrency=self._activity_concurrency,
            activity_executor=activity_executor,
        )
        _dispatcher.register_worker(
            workflows=workflows,
            activities=activities,
            failure_exception_types=workflow_failure_exception_types,
            interceptors=interceptors,
            task_queue=task_queue,
            namespace=namespace,
        )
        # The task queue is a database-backed DBOS queue; this process dequeues
        # only from its listen set. It's registered in run(), since config needs the DB.
        DBOS.listen_queues([task_queue])
        self._shutdown_event: Optional["asyncio.Event"] = None
        self._run_task: Optional["asyncio.Task[None]"] = None
        self._finished = False
        _live_worker = self

    @property
    def task_queue(self) -> str:
        """Task queue this worker is on."""
        return self._task_queue

    @property
    def namespace(self) -> str:
        """Temporal namespace this worker serves (its DBOS system schema)."""
        return self._namespace

    @property
    def is_running(self) -> bool:
        """Whether the worker is running (between run() and shutdown())."""
        return self._shutdown_event is not None

    @property
    def is_shutdown(self) -> bool:
        """Whether the worker has run and shut down. Only ``True`` once the
        worker was started and then fully shut down (mirroring temporalio's
        ``_shutdown_complete_event``); not necessarily ``True`` the instant
        :py:meth:`shutdown` is first called, since the drain takes a moment."""
        return self._finished

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
        # DBOS adopts the running loop, installing its thread pool as the *default
        # executor*. Capture the original so asyncio.to_thread works after exit.
        loop = asyncio.get_running_loop()
        original_executor = getattr(loop, "_default_executor", None)
        DBOS.launch()
        try:
            # Persist this worker's queue config. Per-worker concurrency is the
            # activity cap for an activities-only worker, else the workflow-task cap.
            worker_concurrency = (
                self._activity_concurrency
                if self._activities_only
                else self._max_concurrent_workflow_tasks
            )
            await DBOS.register_queue_async(
                self._task_queue,
                worker_concurrency=worker_concurrency,
                limiter=_rate_limiter(self._activity_rate_per_second),
            )
            await self._shutdown_event.wait()
        except Exception as exc:
            # Surface an unrecoverable worker error to on_fatal_error before it
            # propagates. Normal shutdown / cancellation don't fire the callback.
            if self._on_fatal_error is not None:
                await self._on_fatal_error(exc)
            raise
        finally:
            self._shutdown_event = None
            self._finished = True
            # destroy() can't run on DBOS's adopted loop (self-deadlock); run it on a fresh thread.
            # TODO(dbos-destroy-deadlock): drop this once DBOS.destroy no longer self-deadlocks.
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="dbosify-worker-shutdown"
            ) as shutdown_pool:
                await loop.run_in_executor(
                    shutdown_pool,
                    lambda: DBOS.destroy(
                        workflow_completion_timeout_sec=int(
                            self._graceful_shutdown_timeout.total_seconds()
                        )
                    ),
                )
                # Detach the activity client only AFTER the graceful drain, so a
                # draining activity could still use client(); dispose it off the loop.
                detached_client = _activity._teardown_worker_state()
                if detached_client is not None:
                    await loop.run_in_executor(
                        shutdown_pool, detached_client._dbos_client.destroy
                    )
            if original_executor is None or getattr(
                original_executor, "_shutdown", False
            ):
                # Same construction asyncio uses for its lazy default.
                original_executor = concurrent.futures.ThreadPoolExecutor(
                    thread_name_prefix="asyncio"
                )
            loop.set_default_executor(original_executor)
            _registry.set_worker_deployment_name(None)
            _live_worker = None

    async def shutdown(self) -> None:
        """Initiate shutdown and wait for :py:meth:`run` to return."""
        # Trip activity worker-lifecycle observers NOW so draining activities see
        # it; the client is torn down later so it stays usable while they wind down.
        _activity._signal_worker_shutdown()
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

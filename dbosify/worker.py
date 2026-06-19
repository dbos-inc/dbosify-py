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

Parameter mapping onto DBOS (DEVIATIONS D34): ``max_concurrent_workflow_tasks``
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
# ``workflow.patched()``). See DESIGN §6.8 / DEVIATIONS D28. Distinct apps
# sharing one database should set ``application_version`` explicitly to keep
# their versions apart.
DEFAULT_APP_VERSION = "0.1"

# Worker options that are behavior-changing AND unsupported: passing them (a
# non-default value) raises rather than silently no-ops. arg name -> hint. They
# arrive via the ``**unsupported`` catch-all (not explicit params).
_REJECTED_OPTIONS = {
    "nexus_service_handlers": "Nexus is not supported (DESIGN §1)",
    "tuner": "resource-based slot tuning has no DBOS analog; use "
    "max_concurrent_workflow_tasks / max_concurrent_activities (DEVIATIONS D34)",
    "plugins": "Worker plugins are not supported; use interceptors= (DEVIATIONS D24)",
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
    accepted for parity; AUTO_UPGRADE has no DBOS analog (DEVIATIONS D29).
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
        the cluster routing-fleet / ramping concepts; see DEVIATIONS D29.
        ``use_worker_versioning`` is accepted for parity. When neither build_id
        nor deployment_config is given, the deployment version is derived from
        the DBOS application name + application_version.
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
        # ignored (the accepted-param audit's whole point — DEVIATIONS D34): a
        # user passing these expects an effect we can't deliver.
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
        # Activity concurrency cap: max_concurrent_activities, else (an
        # activities-only worker that set only) max_concurrent_local_activities.
        # In our model regular and local activities both run as steps, sharing
        # one cap (DEVIATIONS D34).
        self._activity_concurrency = (
            max_concurrent_activities
            if max_concurrent_activities is not None
            else max_concurrent_local_activities
        )
        # Whether the worker dequeues activities (vs. only workflows) — its task
        # queue carries __temporal_activity items, so its worker_concurrency is
        # the activity cap rather than the workflow-task cap.
        self._activities_only = bool(activities) and not workflows
        # Activity rate limit → DBOS queue limiter. DBOS's limiter is queue-wide,
        # so the task-queue-wide knob maps exactly; the per-worker knob is applied
        # as a queue-wide approximation when it's the only one set (D34).
        self._activity_rate_per_second = (
            max_task_queue_activities_per_second
            if max_task_queue_activities_per_second is not None
            else max_activities_per_second
        )
        # The interpreter (in this process) decodes run args / encodes results
        # with this converter; configure the Client the same.
        conversion.set_converter(data_converter)
        # The namespace owns the DBOS system schema (DEVIATIONS D1): this
        # process serves one namespace, and its workflows live in that schema —
        # isolated from other namespaces. The Worker owns the runtime, so it
        # sets the schema; a conflicting explicit dbos_system_schema is an error
        # (configure the namespace, not the schema).
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
        # Worker identity → DBOS executor_id (surfaced in DBOS views / list
        # filters). Note: executor_id also *scopes recovery* in DBOS (D6), so a
        # custom identity should be stable per fleet, not unique per process.
        if identity is not None:
            config = {**config, "executor_id": identity}
        # An explicit build_id / deployment_config IS the DBOS application_version
        # (build IDs map to DBOS versions, DEVIATIONS D29): DBOS scopes both
        # workflow recovery and queue dequeue to application_version, so setting
        # it here makes the requested build ID the version DBOS actually pins to
        # — that pinning *is* Temporal's PINNED behavior, enforced. Without an
        # explicit build, pin a stable default so redeploys don't strand in-flight
        # workflows and workflow.patched() reaches pre-patch runs (DEFAULT_APP_VERSION).
        explicit_build = (
            deployment_config.version.build_id
            if deployment_config is not None
            else build_id
        )
        if explicit_build is not None:
            if not explicit_build:
                raise ValueError("build_id must be a non-empty string")
            # A build id IS the DBOS application_version, so a build id alongside
            # *any* explicitly-set application_version (including ``None`` to opt
            # into auto-versioning) is contradictory — check key presence, not
            # just a non-None value, so the auto-versioning combo is caught too.
            if (
                "application_version" in config
                and config["application_version"] != explicit_build
            ):
                raise ValueError(
                    f"build id {explicit_build!r} (from build_id/deployment_config) "
                    f"conflicts with the application_version "
                    f"{config['application_version']!r} already set in the "
                    "DBOSConfig (a build id IS the DBOS application_version); set "
                    "only one"
                )
            config = {**config, "application_version": explicit_build}
        config = _with_default_app_version(config)
        DBOS(config=config)
        # Deployment name surfaced via workflow.Info.get_current_deployment_version():
        # the explicit deployment_config name, else the DBOS app name. The build_id
        # half is read live from the DBOS application_version at access time, so the
        # surfaced version always equals the one DBOS enforces (DEVIATIONS D29).
        _registry.set_worker_deployment_name(
            deployment_config.version.deployment_name
            if deployment_config is not None
            else config.get("name", "")
        )
        # Arm activity worker-lifecycle state (activity.is_worker_shutdown(),
        # activity.client()) for this run, with the activity concurrency cap and
        # sync-activity executor.
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
            # An activities-only worker dequeues __temporal_activity items, so
            # its per-worker concurrency is the activity cap; a workflow worker's
            # is the workflow-task cap.
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
            # propagates (the teardown finally still runs). A normal shutdown
            # returns from wait() without raising, so the callback never fires;
            # cancellation (a BaseException) is excluded.
            if self._on_fatal_error is not None:
                await self._on_fatal_error(exc)
            raise
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
                # Detach the activity client only AFTER the graceful drain above,
                # so an activity reacting to shutdown could still use
                # activity.client() while draining; dispose it off the loop
                # (destroy() does blocking pool I/O), reusing this thread.
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
        # Trip activity worker-lifecycle observers (is_worker_shutdown() /
        # wait_for_worker_shutdown*) NOW, so draining activities can observe it;
        # the activity client is torn down later, after the graceful drain in
        # run()'s finally, so it stays usable while activities wind down.
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

"""Client for starting and interacting with workflows, mirroring
``temporalio.client`` in shape while taking DBOS machinery directly: a
``Client`` wraps a ``dbos.DBOSClient`` (which carries the database URL and
system schema), rather than parsing a Temporal-style target host.

Phase 1 surface: ``start_workflow`` / ``execute_workflow`` /
``get_workflow_handle``, and ``WorkflowHandle`` with ``result``/``signal``/
``query``/``execute_update``/``describe``. Parameters not yet honored are
accepted and ignored with a debug log. ``cancel`` and ``terminate`` land with
the Phase 2 cancellation matrix.
"""

import asyncio
import logging
import os
import uuid as uuid_mod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, List, Mapping, Optional, Sequence, Type, Union

from dbos import DBOSClient, EnqueueOptions, WorkflowStatus
from dbos._error import DBOSAwaitedWorkflowCancelledError

from . import exceptions
from ._internal import ids, inbox
from ._internal import registry as _registry
from ._internal import status as _status
from ._internal.payloads import SerializedWorkflowFailure, deserialize_failure
from ._internal.status import WorkflowExecutionStatus
from .common import RetryPolicy, WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from .workflow import _UpdateMethod

# Worst-case latency for client-side get_event when a LISTEN/NOTIFY wakeup is
# missed (see docs/phase0.md); DBOSClient has no public knob yet.
CLIENT_POLL_ENV = "TEMPORAL_DBOS_CLIENT_POLL_SECONDS"
DEFAULT_CLIENT_POLL_SECONDS = 1.0

__all__ = [
    "Client",
    "WorkflowHandle",
    "WorkflowExecution",
    "WorkflowExecutionDescription",
    "WorkflowExecutionStatus",
    "WorkflowFailureError",
    "WorkflowQueryFailedError",
    "WorkflowUpdateFailedError",
]

logger = logging.getLogger("temporal_dbos.client")

_arg_unset = object()


class WorkflowFailureError(exceptions.TemporalError):
    """The workflow run did not complete successfully; ``cause`` is the
    exact reconstructed failure.
    """

    def __init__(self, *, cause: BaseException) -> None:
        super().__init__("Workflow execution failed")
        self.__cause__ = cause


class WorkflowQueryFailedError(exceptions.TemporalError):
    """The query handler failed or was not found."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class WorkflowUpdateFailedError(exceptions.TemporalError):
    """The update was rejected by its validator or failed in its handler."""

    def __init__(self, cause: BaseException) -> None:
        super().__init__("Workflow update failed")
        self.__cause__ = cause


@dataclass(frozen=True)
class WorkflowExecution:
    """Info for a single workflow execution run (Phase 1 subset of
    temporalio's; field order matches theirs). Constructed by the SDK,
    never by users.
    """

    close_time: Optional[datetime] = None
    id: str = ""
    parent_id: Optional[str] = None
    run_id: str = ""
    start_time: Optional[datetime] = None
    status: Optional[WorkflowExecutionStatus] = None
    task_queue: Optional[str] = None
    workflow_type: str = ""


@dataclass(frozen=True)
class WorkflowExecutionDescription(WorkflowExecution):
    """Description for a single workflow execution run."""


def _workflow_type_name(workflow: Any) -> str:
    """Resolve a workflow reference the temporalio ways: the class, the run
    method (``MyWorkflow.run``), or the type-name string.
    """
    if isinstance(workflow, str):
        return workflow
    if isinstance(workflow, type):
        return _registry.workflow_definition_of(workflow).name
    name = getattr(workflow, _registry.WORKFLOW_NAME_ATTR, None)
    if name is not None:
        return str(name)
    raise TypeError(
        f"Cannot resolve a workflow type from {workflow!r}: pass the "
        "@workflow.defn class, its @workflow.run method, or the type name"
    )


def _signal_name(signal: Any) -> str:
    if isinstance(signal, str):
        return signal
    name = getattr(signal, _registry.SIGNAL_ATTR, None)
    if name is None:
        raise TypeError(f"{signal!r} is not a @workflow.signal method or name")
    return str(name)


def _query_name(query: Any) -> str:
    if isinstance(query, str):
        return query
    name = getattr(query, _registry.QUERY_ATTR, None)
    if name is None:
        raise TypeError(f"{query!r} is not a @workflow.query method or name")
    return str(name)


def _update_name(update: Any) -> str:
    if isinstance(update, str):
        return update
    if isinstance(update, _UpdateMethod):
        return update.name
    raise TypeError(f"{update!r} is not a @workflow.update method or name")


def _resolve_args(arg: Any, args: Sequence[Any]) -> List[Any]:
    if arg is not _arg_unset:
        if args:
            raise ValueError("Cannot have both arg and args")
        return [arg]
    return list(args)


def _to_datetime(epoch_ms: Optional[int]) -> Optional[datetime]:
    if epoch_ms is None:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)


def _ignore_rpc_options(
    where: str, rpc_metadata: Mapping[str, Any], rpc_timeout: Optional[timedelta]
) -> None:
    """RPC transport options have no temporal-dbos equivalent; accept and
    debug-log them (DESIGN convention for tuning parameters)."""
    if rpc_metadata:
        logger.debug("%s: ignoring rpc_metadata", where)
    if rpc_timeout is not None:
        logger.debug("%s: ignoring rpc_timeout", where)


class Client:
    """Client for accessing temporal-dbos, wrapping a ``dbos.DBOSClient``.

    The DBOSClient carries the connection (database URL, system schema), so
    namespacing rides on its ``dbos_system_schema``. Construct directly or
    via the async :py:meth:`connect` (kept for temporalio shape).
    """

    def __init__(self, dbos_client: DBOSClient) -> None:
        self._dbos_client = dbos_client
        # Bound the LISTEN/NOTIFY-miss latency for get_event-based replies
        # (updates/queries). Private until DBOS exposes an option.
        self._dbos_client._sys_db._notification_fallback_polling_interval = float(
            os.environ.get(CLIENT_POLL_ENV, str(DEFAULT_CLIENT_POLL_SECONDS))
        )

    @classmethod
    async def connect(cls, dbos_client: DBOSClient) -> "Client":
        """Create a client from a ``dbos.DBOSClient``."""
        return cls(dbos_client)

    # ------------------------------------------------------------------
    # Workflow start
    # ------------------------------------------------------------------

    async def start_workflow(
        self,
        workflow: Any,
        arg: Any = _arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: Optional[type] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: WorkflowIDReusePolicy = WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: WorkflowIDConflictPolicy = WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[Any] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: Optional[Any] = None,
        request_id: Optional[str] = None,
        **unsupported: Any,
    ) -> "WorkflowHandle":
        """Start a workflow and return its handle.

        Phase 1 honors arg/args, id, task_queue, run_timeout, the
        USE_EXISTING/FAIL conflict policies, the ALLOW_DUPLICATE /
        ALLOW_DUPLICATE_FAILED_ONLY / REJECT_DUPLICATE reuse policies,
        start_delay, and start_signal. Workflow retry_policy and
        cron_schedule are Phase 3.
        """
        for key, value in {
            "result_type": result_type,
            "execution_timeout": execution_timeout,
            "task_timeout": task_timeout,
            "retry_policy": retry_policy,
            "cron_schedule": cron_schedule or None,
            "memo": memo,
            "search_attributes": search_attributes,
            "static_summary": static_summary,
            "static_details": static_details,
            "rpc_metadata": rpc_metadata or None,
            "rpc_timeout": rpc_timeout,
            "request_eager_start": request_eager_start or None,
            "priority": priority,
            "request_id": request_id,
            **unsupported,
        }.items():
            if value is not None:
                logger.debug("start_workflow: ignoring unsupported option %r", key)

        if not task_queue or not isinstance(task_queue, str):
            # Without this, a None/empty queue name would enqueue a workflow
            # no worker can ever dequeue — a silent black hole.
            raise ValueError("task_queue must be a non-empty string")
        type_name = _workflow_type_name(workflow)
        workflow_args = _resolve_args(arg, args)
        ids.validate_workflow_id(id)

        current = await self._current_run(id)
        run_index = 0
        if current is not None:
            current_index, current_status = current
            if _status.is_open(current_status.status):
                # Conflict policies (vs a RUNNING run). There is an inherent
                # TOCTOU window here, accepted for v1 (DESIGN §6.4).
                if id_conflict_policy == WorkflowIDConflictPolicy.USE_EXISTING:
                    return WorkflowHandle(self, id, run_id=current_status.workflow_id)
                raise exceptions.WorkflowAlreadyStartedError(
                    id, type_name, run_id=current_status.workflow_id
                )
            # Reuse policies (vs a closed run).
            if id_reuse_policy == WorkflowIDReusePolicy.REJECT_DUPLICATE:
                raise exceptions.WorkflowAlreadyStartedError(
                    id, type_name, run_id=current_status.workflow_id
                )
            if (
                id_reuse_policy == WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY
                and _status.to_execution_status(current_status.status)
                == WorkflowExecutionStatus.COMPLETED
            ):
                raise exceptions.WorkflowAlreadyStartedError(
                    id, type_name, run_id=current_status.workflow_id
                )
            if id_reuse_policy == WorkflowIDReusePolicy.TERMINATE_IF_RUNNING:
                raise NotImplementedError(
                    "TERMINATE_IF_RUNNING lands in Phase 3 (see README "
                    "compatibility table)"
                )
            run_index = current_index + 1

        dbos_id = ids.run_dbos_id(id, run_index)
        options: EnqueueOptions = {
            "workflow_name": f"wf:{type_name}",
            "queue_name": task_queue,
            "workflow_id": dbos_id,
        }
        if run_timeout is not None:
            options["workflow_timeout"] = run_timeout.total_seconds()
        if start_delay is not None:
            options["delay_seconds"] = start_delay.total_seconds()
        await self._dbos_client.enqueue_async(options, workflow_args)

        if start_signal is not None:
            await self._dbos_client.send_async(
                dbos_id,
                inbox.signal_envelope(start_signal, list(start_signal_args)),
                inbox.INBOX_TOPIC,
            )
        return WorkflowHandle(
            self, id, run_id=dbos_id, first_execution_run_id=ids.run_dbos_id(id, 0)
        )

    async def execute_workflow(
        self,
        workflow: Any,
        arg: Any = _arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: Optional[type] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: WorkflowIDReusePolicy = WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: WorkflowIDConflictPolicy = WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[Any] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: Optional[Any] = None,
        **unsupported: Any,
    ) -> Any:
        """Start a workflow and wait for completion. See ``start_workflow``."""
        handle = await self.start_workflow(
            workflow,
            arg,
            args=args,
            id=id,
            task_queue=task_queue,
            result_type=result_type,
            execution_timeout=execution_timeout,
            run_timeout=run_timeout,
            task_timeout=task_timeout,
            id_reuse_policy=id_reuse_policy,
            id_conflict_policy=id_conflict_policy,
            retry_policy=retry_policy,
            cron_schedule=cron_schedule,
            memo=memo,
            search_attributes=search_attributes,
            static_summary=static_summary,
            static_details=static_details,
            start_delay=start_delay,
            start_signal=start_signal,
            start_signal_args=start_signal_args,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
            request_eager_start=request_eager_start,
            priority=priority,
            **unsupported,
        )
        return await handle.result()

    def get_workflow_handle(
        self,
        workflow_id: str,
        *,
        run_id: Optional[str] = None,
        first_execution_run_id: Optional[str] = None,
        result_type: Optional[type] = None,
    ) -> "WorkflowHandle":
        """Get a workflow handle. With no ``run_id``, operations target the
        latest run of the id at call time.
        """
        return WorkflowHandle(
            self,
            workflow_id,
            run_id=run_id,
            first_execution_run_id=first_execution_run_id,
        )

    def get_workflow_handle_for(
        self,
        workflow: Any,
        workflow_id: str,
        *,
        run_id: Optional[str] = None,
        first_execution_run_id: Optional[str] = None,
    ) -> "WorkflowHandle":
        """Typed variant of :py:meth:`get_workflow_handle`."""
        return self.get_workflow_handle(
            workflow_id, run_id=run_id, first_execution_run_id=first_execution_run_id
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _current_run(
        self, workflow_id: str
    ) -> Optional[tuple[int, WorkflowStatus]]:
        """The highest-index run of a Temporal workflow id, if any."""
        statuses = await self._dbos_client.list_workflows_async(
            workflow_id_prefix=workflow_id
        )
        best: Optional[tuple[int, WorkflowStatus]] = None
        for status in statuses:
            index = ids.run_index_of(workflow_id, status.workflow_id)
            if index is not None and (best is None or index > best[0]):
                best = (index, status)
        return best

    async def _resolve_dbos_id(self, workflow_id: str, run_id: Optional[str]) -> str:
        if run_id is not None:
            return run_id
        current = await self._current_run(workflow_id)
        if current is None:
            raise RuntimeError(f"Workflow not found: {workflow_id!r}")
        return current[1].workflow_id

    async def _status_of(self, dbos_id: str) -> WorkflowStatus:
        statuses = await self._dbos_client.list_workflows_async(workflow_ids=[dbos_id])
        if not statuses:
            raise RuntimeError(f"Workflow run not found: {dbos_id!r}")
        return statuses[0]


class WorkflowHandle:
    """Handle for interacting with a workflow.

    Created via :py:meth:`Client.start_workflow` or
    :py:meth:`Client.get_workflow_handle`.
    """

    def __init__(
        self,
        client: Client,
        id: str,
        *,
        run_id: Optional[str] = None,
        first_execution_run_id: Optional[str] = None,
    ) -> None:
        self._client = client
        self._id = id
        self._run_id = run_id
        self._first_execution_run_id = first_execution_run_id

    @property
    def id(self) -> str:
        """ID of the workflow."""
        return self._id

    @property
    def run_id(self) -> Optional[str]:
        """Run ID this handle is bound to, if any."""
        return self._run_id

    @property
    def first_execution_run_id(self) -> Optional[str]:
        """First run ID of the workflow id's chain, if known."""
        return self._first_execution_run_id

    async def _target(self) -> str:
        return await self._client._resolve_dbos_id(self._id, self._run_id)

    async def result(
        self,
        *,
        follow_runs: bool = True,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any:
        """Wait for and return the result; raises
        :py:class:`WorkflowFailureError` on workflow failure with the exact
        cause reconstructed. (rpc_timeout bounds a single RPC in temporalio,
        not the total wait, so it is accepted and ignored here.)
        """
        _ignore_rpc_options("result", rpc_metadata, rpc_timeout)
        dbos_id = await self._target()
        runtime_client = self._client._dbos_client
        handle: Any = await runtime_client.retrieve_workflow_async(dbos_id)
        try:
            return await handle.get_result()
        except SerializedWorkflowFailure as failure:
            # NOTE: `raise ... from X` overwrites __cause__, which the
            # constructor just set — so the `from` target must be the cause
            # itself.
            cause = deserialize_failure(failure.envelope)
            raise WorkflowFailureError(cause=cause) from cause
        except (asyncio.TimeoutError, DBOSAwaitedWorkflowCancelledError):
            raise
        except Exception as err:
            # FAIL_FAST mode or infrastructure errors: surface with a
            # converted cause rather than a raw pickled exception.
            converted = exceptions.ApplicationError(str(err), type=type(err).__name__)
            converted.__cause__ = err
            raise WorkflowFailureError(cause=converted) from converted

    async def signal(
        self,
        signal: Any,
        arg: Any = _arg_unset,
        *,
        args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Send a signal to the workflow."""
        _ignore_rpc_options("signal", rpc_metadata, None)
        await self._client._dbos_client.send_async(
            await self._target(),
            inbox.signal_envelope(_signal_name(signal), _resolve_args(arg, args)),
            inbox.INBOX_TOPIC,
        )

    async def query(
        self,
        query: Any,
        arg: Any = _arg_unset,
        *,
        args: Sequence[Any] = [],
        result_type: Optional[type] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any:
        """Query the workflow (v1: requires a RUNNING workflow)."""
        _ignore_rpc_options("query", rpc_metadata, None)
        request_id = str(uuid_mod.uuid4())
        client = self._client._dbos_client
        target = await self._target()
        timeout = rpc_timeout.total_seconds() if rpc_timeout else 60.0
        await client.send_async(
            target,
            inbox.query_envelope(
                _query_name(query), _resolve_args(arg, args), request_id
            ),
            inbox.INBOX_TOPIC,
        )
        reply = await client.get_event_async(
            target, inbox.query_result_key(request_id), timeout
        )
        if reply is None:
            raise WorkflowQueryFailedError(
                f"query did not complete within {timeout}s (v1 queries "
                "require a RUNNING workflow; see README deviations)"
            )
        if reply["status"] == "completed":
            return reply["result"]
        raise WorkflowQueryFailedError(str(deserialize_failure(reply["failure"])))

    async def execute_update(
        self,
        update: Any,
        arg: Any = _arg_unset,
        *,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        result_type: Optional[type] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any:
        """Send an update and wait for its result; raises
        :py:class:`WorkflowUpdateFailedError` on rejection or failure.
        """
        _ignore_rpc_options("execute_update", rpc_metadata, None)
        update_id = id or str(uuid_mod.uuid4())
        client = self._client._dbos_client
        target = await self._target()
        timeout = rpc_timeout.total_seconds() if rpc_timeout else 60.0
        await client.send_async(
            target,
            inbox.update_envelope(
                _update_name(update), _resolve_args(arg, args), update_id
            ),
            inbox.INBOX_TOPIC,
            idempotency_key=update_id,
        )
        reply = await client.get_event_async(
            target, inbox.update_result_key(update_id), timeout
        )
        if reply is None:
            raise TimeoutError(f"update did not complete within {timeout}s")
        if reply["status"] == "completed":
            return reply["result"]
        raise WorkflowUpdateFailedError(deserialize_failure(reply["failure"]))

    async def describe(
        self,
        *,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowExecutionDescription:
        """Get the current description of this workflow's latest (or bound)
        run."""
        _ignore_rpc_options("describe", rpc_metadata, rpc_timeout)
        dbos_id = await self._target()
        status = await self._client._status_of(dbos_id)
        workflow_type = status.name or ""
        if workflow_type.startswith("wf:"):
            workflow_type = workflow_type[3:]
        return WorkflowExecutionDescription(
            id=self._id,
            run_id=status.workflow_id,
            workflow_type=workflow_type,
            task_queue=status.queue_name,
            status=_status.to_execution_status(status.status),
            start_time=_to_datetime(status.created_at),
            close_time=_to_datetime(status.completed_at),
            parent_id=status.parent_workflow_id,
        )

    async def cancel(
        self,
        *,
        reason: str = "",
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Cooperative cancellation: Phase 2 (see DESIGN §6.5)."""
        raise NotImplementedError(
            "handle.cancel() lands in Phase 2 (cooperative cancellation)"
        )

    async def terminate(
        self,
        *args: Any,
        reason: Optional[str] = None,
        rpc_metadata: Mapping[str, Any] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Forceful termination: Phase 2 (see DESIGN §6.5)."""
        raise NotImplementedError("handle.terminate() lands in Phase 2")

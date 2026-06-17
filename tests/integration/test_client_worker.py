"""Phase 1 public-API tests: the hello-world quad and friends, written the
way a temporal-dbos app is: a Worker built from a DBOSConfig (owning the
process's DBOS lifecycle), a Client wrapping a DBOSClient.
"""

import asyncio
import warnings
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncIterator, List, Optional

import pytest
from dbos import DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import (
    Client,
    WithStartWorkflowOperation,
    WorkflowExecutionStatus,
    WorkflowFailureError,
    WorkflowQueryRejectedError,
    WorkflowUpdateFailedError,
    WorkflowUpdateStage,
)
from temporal_dbos.common import QueryRejectCondition, WorkflowIDConflictPolicy
from temporal_dbos.exceptions import (
    ApplicationError,
    WorkflowAlreadyStartedError,
)
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "phase1-tq"


@activity.defn
async def compose_greeting(greeting: str, name: str) -> str:
    info = activity.info()
    assert info.activity_type == "compose_greeting"
    assert info.attempt >= 1
    return f"{greeting}, {name}! (wf={info.workflow_id})"


@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        result: str = await workflow.execute_activity(
            compose_greeting,
            args=["Hello", name],
            start_to_close_timeout=timedelta(seconds=10),
        )
        return result


@workflow.defn
class AccumulatorWorkflow:
    def __init__(self) -> None:
        self.total = 0
        self.done = False

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.query
    def total_so_far(self) -> int:
        return self.total

    @workflow.update
    def add(self, n: int) -> int:
        self.total += n
        return self.total

    @add.validator
    def _validate_add(self, n: int) -> None:
        if n < 0:
            raise ApplicationError("no negatives", type="BadAmount")

    @workflow.run
    async def run(self) -> int:
        await workflow.wait_condition(lambda: self.done)
        return self.total


@workflow.defn
class FailingWorkflow:
    @workflow.run
    async def run(self) -> None:
        raise ApplicationError("intentional failure", "some-detail", type="MyError")


@workflow.defn
class SignalStartWorkflow:
    def __init__(self) -> None:
        self.greetings: List[str] = []

    @workflow.signal
    def greet(self, name: str) -> None:
        self.greetings.append(name)

    @workflow.run
    async def run(self) -> List[str]:
        await workflow.wait_condition(lambda: len(self.greetings) >= 2)
        return self.greetings


@workflow.defn
class StagedUpdateWorkflow:
    def __init__(self) -> None:
        self.release = False
        self.done = False

    @workflow.signal
    def unblock(self) -> None:
        self.release = True

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.update
    async def slow_update(self, n: int) -> int:
        await workflow.wait_condition(lambda: self.release)
        return n * 2

    @slow_update.validator
    def _validate_slow_update(self, n: int) -> None:
        if n < 0:
            raise ApplicationError("no negatives", type="Neg")

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(
            lambda: self.done and workflow.all_handlers_finished()
        )
        return "done"


@workflow.defn
class UnfinishedHandlersWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.update
    async def stuck_update(self) -> None:
        await workflow.wait_condition(lambda: False)

    @workflow.signal(unfinished_policy=workflow.HandlerUnfinishedPolicy.ABANDON)
    async def stuck_signal(self) -> None:
        await workflow.wait_condition(lambda: False)

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self.done)
        return "done"


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    """A running Worker plus a Client against the same database. The client
    is created after the worker launches (launch creates the database).
    """
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[
            GreetingWorkflow,
            AccumulatorWorkflow,
            FailingWorkflow,
            SignalStartWorkflow,
            StagedUpdateWorkflow,
            UnfinishedHandlersWorkflow,
        ],
        activities=[compose_greeting],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield await Client.connect(dbos_client)
        finally:
            dbos_client.destroy()


async def test_hello_world_quad() -> None:
    async with _env() as client:
        result = await client.execute_workflow(
            GreetingWorkflow.run, "World", id="hello-wf", task_queue=TASK_QUEUE
        )
    assert result == "Hello, World! (wf=hello-wf)"


async def test_handle_signal_query_update() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            AccumulatorWorkflow.run, id="acc-wf", task_queue=TASK_QUEUE
        )
        assert await handle.execute_update(AccumulatorWorkflow.add, 5) == 5
        assert await handle.execute_update("add", 3) == 8
        with pytest.raises(WorkflowUpdateFailedError) as exc_info:
            await handle.execute_update(AccumulatorWorkflow.add, -1)
        cause = exc_info.value.__cause__
        assert isinstance(cause, ApplicationError) and cause.type == "BadAmount"
        assert await handle.query(AccumulatorWorkflow.total_so_far) == 8
        await handle.signal(AccumulatorWorkflow.finish)
        assert await handle.result() == 8

        description = await handle.describe()
        assert description.status == WorkflowExecutionStatus.COMPLETED
        assert description.workflow_type == "AccumulatorWorkflow"
        assert description.task_queue == TASK_QUEUE


async def test_workflow_failure_reconstructed() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            FailingWorkflow.run, id="fail-wf", task_queue=TASK_QUEUE
        )
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result()
        cause = exc_info.value.cause
        assert isinstance(cause, ApplicationError)
        assert cause.type == "MyError"
        assert cause.message == "intentional failure"
        assert list(cause.details) == ["some-detail"]
        assert (await handle.describe()).status == WorkflowExecutionStatus.FAILED


async def test_id_conflict_and_reuse() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            AccumulatorWorkflow.run, id="reuse-wf", task_queue=TASK_QUEUE
        )
        # Conflict with a RUNNING workflow: Temporal's default behavior fails.
        with pytest.raises(WorkflowAlreadyStartedError):
            await client.start_workflow(
                AccumulatorWorkflow.run, id="reuse-wf", task_queue=TASK_QUEUE
            )
        await handle.signal(AccumulatorWorkflow.finish)
        assert await handle.result() == 0

        # Reuse after close: a new run with a chained run id. The chain
        # this start "begins" is its own run (temporalio semantics).
        second = await client.start_workflow(
            AccumulatorWorkflow.run, id="reuse-wf", task_queue=TASK_QUEUE
        )
        assert second.result_run_id == "reuse-wf--r1"
        assert second.first_execution_run_id == "reuse-wf--r1"
        await second.signal(AccumulatorWorkflow.finish)
        assert await second.result() == 0

        # An unbound handle resolves to the latest run.
        latest = client.get_workflow_handle("reuse-wf")
        assert (await latest.describe()).run_id == "reuse-wf--r1"


async def test_signal_with_start() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            SignalStartWorkflow.run,
            id="sws-wf",
            task_queue=TASK_QUEUE,
            start_signal="greet",
            start_signal_args=["first"],
        )
        await handle.signal(SignalStartWorkflow.greet, "second")
        assert await handle.result() == ["first", "second"]


async def test_signal_with_start_attaches_to_running() -> None:
    # USE_EXISTING signal-with-start against an already-running run must deliver
    # the signal to it, not silently drop it (the early-return-before-send gap).
    # The first call starts the run (atomically delivering "first"); the run
    # blocks waiting for a second greeting, so it's still open when the second
    # call attaches and must deliver "second" to that same run.
    async with _env() as client:
        first = await client.start_workflow(
            SignalStartWorkflow.run,
            id="sws-attach-wf",
            task_queue=TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            start_signal="greet",
            start_signal_args=["first"],
        )
        second = await client.start_workflow(
            SignalStartWorkflow.run,
            id="sws-attach-wf",
            task_queue=TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            start_signal="greet",
            start_signal_args=["second"],
        )
        # The second call attached to the running run rather than starting a new
        # one, and its start signal reached that run (else run() hangs at one
        # greeting): both handles resolve the same result.
        assert await first.result() == ["first", "second"]
        assert await second.result() == ["first", "second"]


async def test_one_worker_per_process() -> None:
    async with _env():
        with pytest.raises(RuntimeError, match="one Worker per process"):
            Worker(
                default_config(),
                task_queue="another-queue",
                workflows=[GreetingWorkflow],
            )


async def test_task_queue_validation() -> None:
    # Worker: validated before any DBOS state is touched (and before the
    # one-worker-per-process check, so bad args always read as bad args).
    with pytest.raises(ValueError, match="task_queue"):
        Worker(default_config(), task_queue="", workflows=[GreetingWorkflow])
    async with _env() as client:
        with pytest.raises(ValueError, match="task_queue"):
            await client.start_workflow(
                GreetingWorkflow.run,
                "x",
                id="tq-none",
                task_queue=None,  # type: ignore[arg-type]
            )
        with pytest.raises(ValueError, match="task_queue"):
            await client.start_workflow(
                GreetingWorkflow.run, "x", id="tq-empty", task_queue=""
            )


async def test_workflow_id_validation() -> None:
    async with _env() as client:
        with pytest.raises(ValueError, match="--r"):
            await client.start_workflow(
                GreetingWorkflow.run, "x", id="bad--r1", task_queue=TASK_QUEUE
            )


async def test_start_update_stages() -> None:
    """start_update(ACCEPTED) returns once past the validator, before the
    handler finishes; the handle's result() collects the eventual value.
    Rejection raises from start_update itself."""
    async with _env() as client:
        handle = await client.start_workflow(
            StagedUpdateWorkflow.run, id="staged-wf", task_queue=TASK_QUEUE
        )
        update_handle = await handle.start_update(
            StagedUpdateWorkflow.slow_update,
            21,
            wait_for_stage=WorkflowUpdateStage.ACCEPTED,
        )
        # Accepted but parked: release the handler, then collect the result.
        await handle.signal(StagedUpdateWorkflow.unblock)
        assert await update_handle.result() == 42

        with pytest.raises(WorkflowUpdateFailedError):
            await handle.start_update(
                StagedUpdateWorkflow.slow_update,
                -1,
                wait_for_stage=WorkflowUpdateStage.ACCEPTED,
            )

        # The run waits on all_handlers_finished before returning.
        await handle.signal(StagedUpdateWorkflow.finish)
        assert await handle.result() == "done"


async def test_update_with_start() -> None:
    """execute_update_with_start_workflow lazily creates the workflow on the
    first call (USE_EXISTING), attaches on subsequent calls, exposes the
    workflow handle on the operation, and operations are single-use."""

    def _op() -> WithStartWorkflowOperation:
        return WithStartWorkflowOperation(
            AccumulatorWorkflow.run,
            id="uws-wf",
            task_queue=TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )

    async with _env() as client:
        first_op = _op()
        assert (
            await client.execute_update_with_start_workflow(
                AccumulatorWorkflow.add, 5, start_workflow_operation=first_op
            )
            == 5
        )
        # Second call attaches to the existing workflow: state accumulates.
        assert (
            await client.execute_update_with_start_workflow(
                AccumulatorWorkflow.add, 3, start_workflow_operation=_op()
            )
            == 8
        )
        # The handle is available on the used operation; single-use enforced.
        handle = await first_op.workflow_handle()
        with pytest.raises(RuntimeError, match="reuse"):
            await client.execute_update_with_start_workflow(
                AccumulatorWorkflow.add, 1, start_workflow_operation=first_op
            )
        await handle.signal(AccumulatorWorkflow.finish)
        assert await handle.result() == 8


async def test_loop_default_executor_survives_worker_exit() -> None:
    """DBOS's async APIs install DBOS's thread pool as the calling loop's
    default executor and destroy() shuts that pool down; the Worker restores
    a live default executor on exit so the application's asyncio.to_thread
    keeps working after `async with Worker(...)`."""
    async with _env() as client:
        # Force executor swaps both in run() and in workflow execution.
        await client.execute_workflow(
            GreetingWorkflow.run, "exec", id="executor-wf", task_queue=TASK_QUEUE
        )
    assert await asyncio.to_thread(lambda: 42) == 42


async def test_unfinished_handler_warnings() -> None:
    """A workflow that reaches a terminal outcome with handlers mid-flight
    warns per WARN_AND_ABANDON handler (Temporal's HandlerUnfinishedPolicy);
    ABANDON handlers are abandoned silently."""
    async with _env() as client:
        handle = await client.start_workflow(
            UnfinishedHandlersWorkflow.run, id="unfinished-wf", task_queue=TASK_QUEUE
        )
        await handle.start_update(
            UnfinishedHandlersWorkflow.stuck_update,
            wait_for_stage=WorkflowUpdateStage.ACCEPTED,
            id="stuck-upd",
        )
        await handle.signal(UnfinishedHandlersWorkflow.stuck_signal)
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            await handle.signal(UnfinishedHandlersWorkflow.finish)
            assert await handle.result() == "done"
        kinds = [type(w.message) for w in captured]
        assert workflow.UnfinishedUpdateHandlersWarning in kinds
        # The stuck signal handler opted out via ABANDON.
        assert workflow.UnfinishedSignalHandlersWarning not in kinds
        message = str(
            next(
                w.message
                for w in captured
                if isinstance(w.message, workflow.UnfinishedUpdateHandlersWarning)
            )
        )
        assert "stuck_update" in message and "stuck-upd" in message
        # The abandoned update fails its caller promptly (Temporal fails
        # accepted-but-incomplete updates at workflow close) rather than
        # leaving it to time out.
        with pytest.raises(WorkflowUpdateFailedError) as upd_err:
            await handle.get_update_handle("stuck-upd").result()
        cause = upd_err.value.__cause__
        assert isinstance(cause, ApplicationError)
        assert cause.type == "AcceptedUpdateCompletedWorkflow"


async def test_query_reject_condition() -> None:
    """reject_condition (per-call or client default) rejects queries by
    workflow status before sending, raising WorkflowQueryRejectedError."""
    async with _env() as client:
        handle = await client.start_workflow(
            AccumulatorWorkflow.run, id="qrc-wf", task_queue=TASK_QUEUE
        )
        # Open workflow: the condition passes and the query runs.
        total = await handle.query(
            AccumulatorWorkflow.total_so_far,
            reject_condition=QueryRejectCondition.NOT_OPEN,
        )
        assert total == 0
        await handle.signal(AccumulatorWorkflow.finish)
        await handle.result()
        with pytest.raises(WorkflowQueryRejectedError) as exc_info:
            await handle.query(
                AccumulatorWorkflow.total_so_far,
                reject_condition=QueryRejectCondition.NOT_OPEN,
            )
        assert exc_info.value.status == WorkflowExecutionStatus.COMPLETED

        # The client-level default applies when the call passes nothing.
        strict_client = await Client.connect(
            client._dbos_client,
            default_workflow_query_reject_condition=QueryRejectCondition.NOT_OPEN,
        )
        strict_handle = strict_client.get_workflow_handle("qrc-wf")
        with pytest.raises(WorkflowQueryRejectedError):
            await strict_handle.query(AccumulatorWorkflow.total_so_far)
